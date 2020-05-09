#!/usr/bin/python3
#
# Library wrapping qsub for control of files and output in python 3.8
#
# If launched directly it launches four times the hostname_example.sge
# SGE script
#
# Copyright(C) Pablo Martin Medrano <pablo@odkq.com>
# MIT License. See LICENSE file
#
import os
import time
from subprocess import Popen, PIPE
from enum import Enum


class JobStatus(Enum):
    ''' Possible states of a job '''
    FAILED = 1    # Submission failed
    RUNNING = 2   # Queued (or running)
    FINISHED = 3  # Finished (the output can be read) without aparent errors
    ERROR = 4     # Finished but with an error in stderr


class Job:
    ''' Struct for the submitted job. Our index, the job_id given by the
        queue, its output file and the status '''
    def __init__(self, index, sge_script, job_id, output_file, error_file):
        self.index = index
        self.job_id = job_id
        self.sge_script = sge_script
        self.output_file = output_file
        self.error_file = error_file
        if self.job_id == -1:
            self.status = JobStatus.FAILED
        else:
            self.status = JobStatus.RUNNING
        self.output_string = ''


class Submitter:
    ''' Interact with the job queue, monitoring job output files '''
    def __init__(self):
        self.jobs = []
        self.current_index = 0

    def __submit_sge_script(self, name, output, error):
        ''' Submit the given SGE script to the job queue with the given output
            file '''
        p = Popen(['qsub', '-o', output, '-e', error, name], stdout=PIPE,
                  stdin=PIPE, stderr=PIPE)
        stdout_data, stderr_data = p.communicate()

        # Look for the job id in the output
        first = stdout_data.find(b'Your job ')
        if first == -1:
            return -1
        first += len(b'Your job ')
        last = stdout_data[first:].find(b' ') + first
        job_id = int(stdout_data[first:last])
        return job_id

    def submit(self, name):
        ''' Submit a sge script. The output file name is generated
        automatically '''
        index = self.current_index
        output_file = 'output_' + str(index)
        error_file = 'error_' + str(index)
        self.current_index += 1
        job_id = self.__submit_sge_script(name, output_file, error_file)
        print('Submitted job {} with index {}'.format(index, job_id))
        job = Job(index, name, job_id, output_file, error_file)
        self.jobs.append(job)
        return job

    def __check_for_finish(self, job_index):
        ''' Check if a given job (by index) has finished either OK or
            failed '''
        job = self.jobs[job_index]
        if job.status != JobStatus.RUNNING:
            return True

        if not os.path.isfile(job.output_file):
            return False

        os.sync()
        # If the output file is empty, wait a bit
        if os.stat(job.output_file).st_size == 0:
            return False

        if os.path.isfile(job.error_file):
            if os.stat(job.error_file).st_size != 0:
                # Prepend the error output. Do not consider that the
                # existance of a error file with more than one byte means
                # job finished with error, as the output for the time
                # command goes to stderr
                ob.output_string += open(job.error_file, 'r').read()
                os.remove(job.error_file)
            else:
                job.status = JobStatus.FINISHED
        job.output_string += open(job.output_file, 'r').read()
        os.remove(job.output_file)
        print('Job {} ({}) finished'.format(job_index, job.job_id))
        job.status = JobStatus.FINISHED
        return True

    def __check_finished_jobs(self):
        ''' Return True if all jobs have finished, with error or success '''
        finished = True
        for i in range(self.current_index):
            if self.__check_for_finish(i) is False:
                finished = False
        return finished

    def wait(self):
        ''' Wait until all jobs have finished, checking once a second '''
        while self.__check_finished_jobs() is False:
            time.sleep(1)

    def __print_job(self, index, print_output=True):
        job = self.jobs[index]
        fmtstr = 'Job index {} SGE script {} Job Id {} status {}'
        print(fmtstr.format(job.index, job.sge_script, job.job_id,
                            job.status))
        if print_output:
            if job.status in [JobStatus.FINISHED, JobStatus.ERROR]:
                print('  Output: {}'.format(job.output_string))

    def print_results(self, print_output=True):
        for i in range(self.current_index):
            self.__print_job(i, print_output)


if __name__ == "__main__":
    submitter = Submitter()

    for i in range(4):
        submitter.submit('hostname_example.sge')

    submitter.wait()
    submitter.print_results()
