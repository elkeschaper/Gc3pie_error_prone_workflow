#! /usr/bin/env python

import os
import os.path
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

############################# Basic Applications/Tasks ###################################


class A(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("A")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/myscript.py", "-o", "/path/to/A/out"],
                                     inputs = [],
                                     outputs = [],
                                     #join = True,
                                     stdout = "stdout.log",
                                     stderr = "stderr.log",
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("A is NEVER done.")
        self.execution.returncode = 1
        self.whatever = 10

        # read logfile
        with open(os.path.join(self.output_directory, self.stdout), "r"):
            1==1
            #do stuff

        # delete log files

        # check whether output file exists.
        # otherwise: exitstatus to failure.

        self.execution.state = [TERMINATED, RUNNING, STOPPED, SUBMITTED]
        self.execution.returncode = []
        self.error = last_line_of_log_file


class B(Application):
    def __init__(self, joke, path_to_A_files, **kwargs):

        gc3libs.log.info("B")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/myscript.py", "-i", os.path.join(path_to_A_files, joke), "$RANDOM"],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = "./results/B_{}".format(joke),
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("B is done.")





class C(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("C")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/hostname"],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = "./results/C_{}".format(joke),
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("C is done.")


class D(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("D")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/ps"],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = "./results/D_{}".format(joke),
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("D is done.")


############################# Main Session Creator (?) ###################################


class TestWorkflow(SessionBasedScript):
    """
    Test pipeline
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.0.1',
            )

    def setup_options(self):
        self.add_param("-j", "--jokes", type=str, nargs="+",
                       help="List of jokes")

    def _make_session()

    def parse_args(self):
        self.jokes = self.params.jokes
        gc3libs.log.info("TestWorkflow Jokes: {}".format(self.jokes))


    def new_tasks(self, kwargs):

        #name = "myTestWorkflow"
        gc3libs.log.info("Calling TestWorkflow.next_tasks()")

        yield test_workflow.MainSequentialFlow(self.jokes, **kwargs)


######################## Support Classes / Workflow elements #############################


class MainSequentialFlow(SequentialTaskCollection):
    def __init__(self, jokes, **kwargs):
        self.jokes = jokes

        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format(jokes))

        self.initial_task = A(jokes)

        SequentialTaskCollection.__init__(self, [self.initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            self.add(MainParallelFlow(self.jokes))
            return Run.State.RUNNING
        elif iterator == 1:
            self.add(D(self.jokes))
            return Run.State.RUNNING
        else:
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t MainSequentialFlow.terminated [%s]" % self.execution.returncode)


class MainParallelFlow(ParallelTaskCollection):

    def __init__(self, jokes, **kwargs):

        self.jokes = jokes
        gc3libs.log.info("\t\tCalling MainParallelFlow.__init({})".format(self.jokes))

        self.tasks = [InnerSequentialFlow(joke) for joke in self.jokes]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tMainParallelFlow.terminated")



class InnerSequentialFlow(SequentialTaskCollection):
    def __init__(self, joke, **kwargs):

        self.joke = joke
        gc3libs.log.info("\t\t\t\tCalling InnerSequentialFlow.__init__ for joke: {}".format(self.joke))

        self.job_name = joke
        initial_task = B(joke)
        SequentialTaskCollection.__init__(self, [initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            gc3libs.log.info("\t\t\t\tCalling InnerSequentialFlow.next(%d) ... " % int(iterator))
            self.add(C(self.joke))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t\t\t\tInnerSequentialFlow.terminated [%d]" % self.execution.returncode)




# run script
if __name__ == '__main__':
    import test_workflow
    TestWorkflow().run()