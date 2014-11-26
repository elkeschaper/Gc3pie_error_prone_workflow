#! /usr/bin/env python

import configobj
import os
import os.path
import re
import sqlalchemy as sqla
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.persistence.accessors import GetValue
import gc3libs.debug
import gc3libs.utils

config_file = '/Users/elkeschaper/Python_projects/Gc3pie_error_prone_workflow/tandem_repeat_annotation_defaults.ini'
config = configobj.ConfigObj(config_file, stringify=True)
#self.c = configobj.ConfigObj(config_file, configspeself.c = config_specs, stringify=True)

######################## Basic Applications/Tasks Templates ##############################

class MyApplication(Application):
    """
    Basic template method pattern  `Application`:class: Initialise Application generically,
    and check for successful running generically.
    """
    @gc3libs.debug.trace
    def __init__(self, name, **kwargs):

        gc3libs.log.info("Initialising {}".format(self.__class__.__name__))
        self.c = config[name]

        # Replace every "%X" in the config with the current value for X, e.g. "3".
        if "param" in kwargs:
            for iC in self.c.keys():
                for param_name,param_value in kwargs['param'].items():
                    self.c[iC] = self.c[iC].replace(param_name, param_value)

        gc3libs.Application.__init__(self,
                                     arguments = [self.c['script'], "-i", self.c['input'], "-o", self.c['output'], self.c['extra']],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = self.c['stdout'],
                                     stderr = self.c['stderr'],
                                     output_dir = self.c['logdir'],
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("Testing whether {} is done".format(self.__class__.__name__))

        # If the application has terminated o.k. (self.execution.returncode == 0),
        #   Check wether all resultfiles are o.k. If yes: Good, If no: FREEZE.
        # If not: Save error and FREEZE.

        if self.execution.returncode == 0:
            gc3libs.log.info("{1} claims to be successful: self.execution.returncode: {0}".format(self.execution.returncode, self.__class__.__name__))
            # Check if result file exists (Later: and complies to some rules).
            if self.is_valid(self.c['output']):  # IMPLEMENT IN CLASS!
                gc3libs.log.info("{} has run successfully to completion.".format(self.__class__.__name__))
                # Now, clean up
                # Delete all log files if everything ran smoothly.
#                os.remove(os.path.join(self.output_dir, self.c['stdout']))
#                os.remove(os.path.join(self.output_dir, self.c['stderr']))
            else:
                gc3libs.log.info("%s has not produced a valid outputfile.", self.__class__.__name__)
                # Set self.execution.exitcode to a non-zero integer <256 (to indicate an error)
                self.execution.exitcode = 42

        else:
            gc3libs.log.info("{1} is not successful: self.execution.returncode: {0}".format(self.execution.returncode, self.__class__.__name__))
            # Check if there is stderr.
            if not os.path.isfile(self.c['stderr']):
                self.error_tag = ""
            else:
                # Create a tag from the last line in stderr.
                with open(os.path.join(self.output_directory, self.stderr), "r") as fh:
                    for line in fh:
                        pass
                    self.error_tag = line
            self.execution.exitcode = 42


class StopOnError(object):
    """
    Mix-in class to make a `SequentialTaskCollection`:class: turn to STOPPED
    state as soon as one of the tasks fail.
    """
    def next(self, done):
        if done == len(self.tasks) - 1:
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED
        else:
            rc = self.tasks[done].execution.exitcode
            if rc != 0:
                return Run.State.STOPPED
            else:
                return Run.State.RUNNING


################################# Applications/Tasks #####################################

class SplitSequenceFile(MyApplication):

    def is_valid(self, output):

#        if not os.path.isfile(self.c['output']):
#        gc3libs.log.info("{} has not produced an output file.".format(self.__class__.__name__))
#        self.execution.returncode = 1
#        # Set self.execution.exitcode to a non-zero integer <256 (to indicate an error)
#        self.execution.exitcode = 42

        return True


class CreateAnnotateSequencePickle(MyApplication):
    def is_valid(self, output):
        return True


class CreateHMMPickles(MyApplication):

    def is_valid(self, output):
        return True


class AnnotateTRsFromHmmer(MyApplication):

    def is_valid(self, output):
        return True


class AnnotateDeNovo(MyApplication):

    def is_valid(self, output):
        return True


class CalculateSignificance(MyApplication):

    def is_valid(self, output):
        return True


class MergeAndBasicFilter(MyApplication):

    def is_valid(self, output):
        return True


class CalculateOverlap(MyApplication):

    def is_valid(self, output):
        return True


class RefineDenovo(MyApplication):

    def is_valid(self, output):
        return True


class SerializeAnnotations(MyApplication):

    def is_valid(self, output):
        return True


############################# Main Session Creator (?) ###################################


class TandemRepeatAnnotationWorkflow(SessionBasedScript):
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

    def _make_session(self, session_uri, store_url):
        return gc3libs.session.Session(
            session_uri,
            store_url,
            extra_fields = {
                # NB: enlarge window to at least 150 columns to read this table properly!
                sqla.Column('class',              sqla.TEXT())    : (lambda obj: obj.__class__.__name__)                                              , # task class
                sqla.Column('name',               sqla.TEXT())    : GetValue()             .jobname                                                   , # job name
                sqla.Column('executable',         sqla.TEXT())    : GetValue(default=None) .arguments[0]                        ,#.ONLY(CodemlApplication), # program executable
                sqla.Column('output_path',        sqla.TEXT())    : GetValue(default=None) .output_dir                        ,#.ONLY(CodemlApplication), # fullpath to codeml output directory
                sqla.Column('cluster',            sqla.TEXT())    : GetValue(default=None) .execution.resource_name           ,#.ONLY(CodemlApplication), # cluster/compute element
                sqla.Column('worker',             sqla.TEXT())    : GetValue(default=None) .hostname                          ,#.ONLY(CodemlApplication), # hostname of the worker node
                sqla.Column('cpu',                sqla.TEXT())    : GetValue(default=None) .cpuinfo                           ,#.ONLY(CodemlApplication), # CPU model of the worker node
                sqla.Column('requested_walltime', sqla.INTEGER()) : _get_requested_walltime_or_none                           , # requested walltime, in hours
                sqla.Column('requested_cores',    sqla.INTEGER()) : GetValue(default=None) .requested_cores                   ,#.ONLY(CodemlApplication), # num of cores requested
                sqla.Column('used_walltime',      sqla.INTEGER()) : GetValue(default=None) .execution.used_walltime           ,#.ONLY(CodemlApplication), # used walltime
                sqla.Column('lrms_jobid',         sqla.TEXT())    : GetValue(default=None) .execution.lrms_jobid              ,#.ONLY(CodemlApplication), # arc job ID
                sqla.Column('original_exitcode',  sqla.INTEGER()) : GetValue(default=None) .execution.original_exitcode       ,#.ONLY(CodemlApplication), # original exitcode
                sqla.Column('used_cputime',       sqla.INTEGER()) : GetValue(default=None) .execution.used_cputime            ,#.ONLY(CodemlApplication), # used cputime in sec
                # returncode = exitcode*256 + signal
                sqla.Column('returncode',         sqla.INTEGER()) : GetValue(default=None) .execution.returncode              ,#.ONLY(CodemlApplication), # returncode attr
                sqla.Column('queue',              sqla.TEXT())    : GetValue(default=None) .execution.queue                   ,#.ONLY(CodemlApplication), # exec queue _name_
                sqla.Column('time_submitted',     sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['SUBMITTED']  ,#.ONLY(CodemlApplication), # client-side submission (float) time
                sqla.Column('time_terminated',    sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['TERMINATED'] ,#.ONLY(CodemlApplication), # client-side termination (float) time
                sqla.Column('time_stopped',       sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['STOPPED']    ,#.ONLY(CodemlApplication), # client-side stop (float) time
                sqla.Column('error_tag',          sqla.TEXT())    : GetValue(default=None) .error_tag
                })

    def parse_args(self):
        self.jokes = self.params.jokes
        gc3libs.log.info("TestWorkflow Jokes: {}".format(self.jokes))


    def new_tasks(self, kwargs):

        #name = "myTestWorkflow"
        gc3libs.log.info("Calling TestWorkflow.next_tasks()")

        yield tandem_repeat_annotation_workflow.MainSequentialFlow(**kwargs)




######################## Support Classes / Workflow elements #############################


#class MainSequentialFlow(StopOnError, SequentialTaskCollection):
class MainSequentialFlow(SequentialTaskCollection):
    def __init__(self, **kwargs):

        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format("<No parameters>"))

        if config["create_hmm_pickles"]["activated"] == 'True':
            self.initial_tasks = [DataPreparationParallelFlow()]
        else:
            self.initial_tasks = [SeqPreparationSequential()]

        ## What does this line do??????????
        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def next(self, iterator):
        # Mixture of enumerating tasks, and StopOnError
        if self.tasks[iterator].execution.exitcode != 0:
            return Run.State.STOPPED # == 'STOPPED'
        elif iterator == 0:
            self.add(SequencewiseParallelFlow())
            return Run.State.RUNNING
        elif iterator == 1:
            self.add(SerializeAnnotations())
            return Run.State.RUNNING
        else:
            return Run.State.TERMINATED


    def terminated(self):
        gc3libs.log.info("\t MainSequentialFlow.terminated [%s]" % self.execution.returncode)


class DataPreparationParallelFlow(ParallelTaskCollection):

    def __init__(self, **kwargs):

        self.kwargs = kwargs
        gc3libs.log.info("\t\tCalling DataPreparationParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [SeqPreparationSequential(),CreateHMMPickles(name = 'create_hmm_pickles')]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tDataPreparationParallelFlow.terminated")


class SeqPreparationSequential(StopOnError, SequentialTaskCollection):
    def __init__(self, **kwargs):

        gc3libs.log.info("\t\t\t\tCalling SeqPreparationSequential.__init__ ")

        self.initial_tasks = [SplitSequenceFile(name = "split_sequence_file", **kwargs),
                                CreateAnnotateSequencePickle(name = "create_and_annotate_sequence_pickles", **kwargs)
                                ]

        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        gc3libs.log.info("\t\t\t\tSeqPreparationSequential.terminated [%d]" % self.execution.returncode)


class SequencewiseParallelFlow(ParallelTaskCollection):

    def __init__(self, **kwargs):

        self.c = config["sequencewise_parallel_flow"]

        # TODO: Find all files in dir and create self.lSeq! Warning! Should be done once the
        # Tasks before are finished.
        self.lSeq = [re.findall(self.c['retag'], i)[0] for i in os.listdir(self.c['input'])]
        self.kwargs = kwargs

        gc3libs.log.info("\t\tCalling SequencewiseParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [SequenceSequential(iSeq = iSeq) for iSeq in self.lSeq]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tSequencewiseParallelFlow.terminated")


class SequenceSequential(StopOnError, SequentialTaskCollection):
    def __init__(self, iSeq, **kwargs):

        param = {"$N": iSeq}
        self.iSeq = iSeq
        self.initial_tasks = [TRDwiseParallelFlow(iSeq = iSeq),
                                MergeAndBasicFilter(name = "merge_and_basic_filter", param = param),
                                CalculateOverlap(name = "calculate_overlap", param = param),
                                RefineDenovo(name = "refine_denovo", param = param),
                                ]

        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        gc3libs.log.info("\t\t\t\tTRDSequential.terminated [%d]" % self.execution.returncode)


class TRDwiseParallelFlow(ParallelTaskCollection):

    def __init__(self, iSeq, **kwargs):

        self.c = config["TRDwise_parallel_flow"]
        print(self.c)
        self.iSeq = iSeq
        self.kwargs = kwargs
        gc3libs.log.info("\t\tCalling TRDwiseParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [TRDSequential(n = self.iSeq, TRD = iTRD, TRD_type = iType, **kwargs) for iTRD, iType in self.c.items()]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tTRDwiseParallelFlow.terminated")


class TRDSequential(StopOnError, SequentialTaskCollection):
    @gc3libs.debug.trace
    def __init__(self, n, TRD, TRD_type, **kwargs):

        self.param = {"$N": n, "$TRD": TRD}
        self.n = n
        self.TRD = TRD
        self.TRD_type = TRD_type
        gc3libs.log.info(TRD_type)

        if self.TRD_type == 'Hmmer':
            self.initial_tasks = [AnnotateTRsFromHmmer(name = "annotate_TRs_from_hmmer", param = self.param),
                                CalculateSignificance(name = "calculate_significance", param = self.param),
                                ]
        elif self.TRD_type == 'deNovo':
                    self.initial_tasks = [AnnotateDeNovo(name = "annotate_de_novo", param = self.param),
                                CalculateSignificance(name = "calculate_significance", param = self.param),
                                ]
        else:
            # FIXME!
            raise("TRD_type not known: {}".format(self.TRD_type))
        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        gc3libs.log.info("\t\t\t\tTRDSequential.terminated [%d]" % self.execution.returncode)


def _get_requested_walltime_or_none(job):
    if isinstance(job, gc3libs.application.codeml.CodemlApplication):
        return job.requested_walltime.amount(hours)
    else:
        return None


# run script
if __name__ == '__main__':
    import tandem_repeat_annotation_workflow
    TandemRepeatAnnotationWorkflow().run()