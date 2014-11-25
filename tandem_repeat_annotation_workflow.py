#! /usr/bin/env python

import configobj
import os
import os.path
import sqlalchemy as sqla
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.persistence.accessors import GetValue
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
    def __init__(self, name, **kwargs):

        gc3libs.log.info("Initialising {}".format(self.__class__.__name__))
        self.c = config[name]

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
            if not os.path.isfile(self.c['output']):
                gc3libs.log.info("{} has not produced an output file.".format(self.__class__.__name__))
                self.execution.returncode = 1
                # Set self.execution.exitcode to a non-zero integer <256 (to indicate an error)
                self.execution.exitcode = 42
            else:
                if self.valid(self.c['output']):  # IMPLEMENT IN CLASS!
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
       rc = self.tasks[done].execution.exitcode
       if rc != 0:
           return Run.State.STOPPED # == 'STOPPED'
       else:
           return Run.State.RUNNING


################################# Applications/Tasks #####################################

class SplitSequenceFile(MyApplication):

    def valid(self, output):
        return True


class CreateAnnotateSequencePickle(Application):
    def valid(self, output):
        return True


class CreateHMMPickles(Application):

    def valid(self, output):
        return True


class AnnotateTRsFromHmmer(Application):

    def valid(self, output):
        return True


class AnnotateDeNovo(Application):

    def valid(self, output):
        return True


class CalculateSignificance(Application):

    def valid(self, output):
        return True


class MergeAndBasicFilter(Application):

    def valid(self, output):
        return True


class CalculateOverlap(Application):

    def valid(self, output):
        return True


class RefineDenovo(Application):

    def valid(self, output):
        return True


class SerializeAnnotations(Application):

    def valid(self, output):
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

        yield tandem_repeat_annotation_workflow.MainSequentialFlow(self.jokes, **kwargs)




######################## Support Classes / Workflow elements #############################


class MainSequentialFlow(StopOnError, SequentialTaskCollection):
    def __init__(self, jokes, **kwargs):
        self.jokes = jokes

        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format(jokes))

        self.initial_tasks = [DataPreparationParallelFlow(),
                    SequencewiseParallelFlow(self.lSeq, self.dTRDAnnotation)
                    ,

                    # Add all the others
                    ]

        ## What does this line do??????????
        SequentialTaskCollection.__init__(self, [self.initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            self.add(SequencewiseParallelFlow(self.lSeq, self.dTRDAnnotation))
            return Run.State.RUNNING
        elif iterator == 1:
            self.add(MergeAndBasicFilter(name = "merge_and_basic_filter"))
            return Run.State.RUNNING
        elif iterator == 2:
            self.add(CalculateOverlap(name = "calculate_overlap"))
            return Run.State.RUNNING
        elif iterator == 3:
            self.add(RefineDenovo(name = "refine_denovo"))
            return Run.State.RUNNING
        elif iterator == 4:
            self.add(SerializeAnnotations(name = "serialize_annotations"))
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


class SeqPreparationSequential(SequentialTaskCollection):
    def __init__(self, **kwargs):

        self.joke = 'hard_coded_joke'
        gc3libs.log.info("\t\t\t\tCalling SeqPreparationSequential.__init__ for joke: {}".format(self.joke))


        self.job_name = self.joke
        initial_task = SplitSequenceFile(self.joke)
        SequentialTaskCollection.__init__(self, [initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            gc3libs.log.info("\t\t\t\tCalling SeqPreparationSequential.next(%d) ... " % int(iterator))
            self.add(CreateAnnotateSequencePickle())
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t\t\t\tSeqPreparationSequential.terminated [%d]" % self.execution.returncode)


class SequencewiseParallelFlow(ParallelTaskCollection):

    def __init__(self, lSeq, dTRDAnnotation, **kwargs):

        # Alternative: Find all files in dir
        self.lSeq = lSeq
        self.dTRDAnnotation = dTRDAnnotation
        gc3libs.log.info("\t\tCalling SequencewiseParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [TRDwiseParallelFlow(iSeq, dTRDAnnotation) for iSeq in lSeq]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tSequencewiseParallelFlow.terminated")


class TRDwiseParallelFlow(ParallelTaskCollection):

    def __init__(self, s, dTRDAnnotation, **kwargs):

        self.s = s
        gc3libs.log.info("\t\tCalling TRDwiseParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [TRDSequential(self.s, iName, iType) for iName, iType in dTRDAnnotation.items()]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tTRDwiseParallelFlow.terminated")


class TRDSequential(SequentialTaskCollection):
    def __init__(self, s, name, type, **kwargs):

        self.s = s
        self.name = name
        self.type = type
        gc3libs.log.info("\t\t\t\tCalling TRDSequential.__init__ for joke: {}".format(self.joke))

        if self.type == 'Hmmer':
            initial_task = AnnotateTRsFromHmmer(name = "annotate_TRs_from_hmmer", s = self.s)
        elif self.type == 'deNovo':
            initial_task = AnnotateDeNovo(name = "annotate_de_novo", TRD = self.TRD)
        else:
            raise("type not known: {}".format(self.type))
        SequentialTaskCollection.__init__(self, [initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            gc3libs.log.info("\t\t\t\tCalling TRDSequential.next(%d) ... " % int(iterator))
            self.add(CalculateSignificance(name = "calculate_significance", n = self.n, notsure = self.name))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

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