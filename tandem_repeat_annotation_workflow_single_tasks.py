#! /usr/bin/env python

import configobj
import os
import os.path
import re
import shlex
import shutil
import sqlalchemy as sqla
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.persistence.accessors import GetValue
from gc3libs.quantity import kB, MB, GB
import gc3libs.debug
import gc3libs.utils


######################## Basic Applications/Tasks Templates ##############################

class MyApplication(Application):
    """
    Basic template method pattern  `Application`:class: Initialise Application generically,
    and check for successful running generically.
    """
    #@gc3libs.debug.trace
    def __init__(self, name, **kwargs):

        gc3libs.log.info("Initialising {}".format(self.__class__.__name__))
        config = kwargs["config"]
        self.c = config[name].copy()
        self.jobname = self.__class__.__name__ # Used by GC3PIE

        kwargs['output_dir'] = self.c['logdir']

        # Replace every "%X" in the config with the current value for X, e.g. "3".
        if "param" in kwargs:
            for iC in self.c.keys():
                for param_name,param_value in kwargs['param'].items():
                    self.c[iC] = self.c[iC].replace(param_name, param_value)

            for param_name,param_value in kwargs['param'].items():
                if param_name == '$TRD':
                    self.TRD = param_value
                elif param_name == '$N':
                    self.N = param_value
                elif param_name == '$BATCH':
                    self.batch = param_value
                # Adapt output_dir to particular file
                kwargs['output_dir'] = kwargs['output_dir'].replace(param_name, param_value)

        if "required_memory" in self.c:
            kwargs['requested_memory'] = int(self.c['required_memory'])*GB
        else:
            kwargs['requested_memory'] = int(config['required_memory'])*GB

        gc3libs.Application.__init__(self,
                                     arguments = shlex.split(self.c['script']) + ["-i"] + shlex.split(self.c['input']) +
                                     ["-o"] + shlex.split(self.c['output']) + shlex.split(self.c['extra']),
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = self.c['stdout'],
                                     stderr = self.c['stderr'],
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
                shutil.rmtree(self.output_dir)
            else:
                gc3libs.log.info("%s has not produced a valid outputfile.", self.__class__.__name__)
                # Set self.execution.exitcode to a non-zero integer <256 (to indicate an error)
                self.execution.exitcode = 42

        else:
            gc3libs.log.info("{1} is not successful: self.execution.returncode: {0}".format(self.execution.returncode, self.__class__.__name__))
            # Check if there is stderr.
            if not os.path.isfile(os.path.join(self.output_dir, self.stderr)):
                self.error_tag = ""
            else:
                # Create a tag from the last line in stderr.
                with open(os.path.join(self.output_dir, self.stderr), "r") as fh:
                    for line in fh:
                        pass
                    self.error_tag = line
            self.execution.exitcode = 88


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
        self.add_param("-conf", "--config_file", type=str, required=True,
                       help="Path to the parameter file")

    def _make_session(self, session_uri, store_url):
        return gc3libs.session.Session(
            session_uri,
            store_url,
            extra_fields = {
                # NB: enlarge window to at least 150 columns to read this table properly!
                sqla.Column('jobname',              sqla.TEXT())    : (lambda obj: obj.__class__.__name__),   # Former: Class                                           , # task class
                sqla.Column('executable',         sqla.TEXT())    : GetValue(default=None) .arguments[0]                        ,#.ONLY(CodemlApplication), # program executable
                sqla.Column('output_path',        sqla.TEXT())    : GetValue(default=None) .output_dir                        ,#.ONLY(CodemlApplication), # fullpath to codeml output directory
                sqla.Column('cluster',            sqla.TEXT())    : GetValue(default=None) .execution.resource_name           ,#.ONLY(CodemlApplication), # cluster/compute element
                ###sqla.Column('worker',             sqla.TEXT())    : GetValue(default=None) .hostname                          ,#.ONLY(CodemlApplication), # hostname of the worker node
                ###sqla.Column('cpu',                sqla.TEXT())    : GetValue(default=None) .cpuinfo                           ,#.ONLY(CodemlApplication), # CPU model of the worker node
                ###sqla.Column('requested_walltime', sqla.INTEGER()) : _get_requested_walltime_or_none                           , # requested walltime, in hours
                sqla.Column('requested_cores',    sqla.INTEGER()) : GetValue(default=None) .requested_cores                   ,#.ONLY(CodemlApplication), # num of cores requested
                sqla.Column('duration_s',         sqla.INTEGER()) : lambda app: app.execution.duration.amount(unit=app.execution.duration.second)           ,#.ONLY(CodemlApplication), # used walltime
                sqla.Column('max_used_memory_mb',      sqla.INTEGER()) : lambda app: app.execution.max_used_memory.amount(unit=app.execution.max_used_memory.MB)           ,#.ONLY(CodemlApplication), # used max_used_memory # might need to be saved as TEXT
                sqla.Column('lrms_jobid',         sqla.TEXT())    : GetValue(default=None) .execution.lrms_jobid              ,#.ONLY(CodemlApplication), # arc job ID
                ###sqla.Column('original_exitcode',  sqla.INTEGER()) : GetValue(default=None) .execution.original_exitcode       ,#.ONLY(CodemlApplication), # original exitcode
                sqla.Column('used_cpu_time_s',         sqla.INTEGER()) : lambda app: app.execution.used_cpu_time.amount(unit=app.execution.used_cpu_time.second)           ,#.ONLY(CodemlApplication), # used walltime
                # returncode = exitcode*256 + signal
                sqla.Column('returncode',         sqla.INTEGER()) : GetValue(default=None) .execution.returncode              ,#.ONLY(CodemlApplication), # returncode attr
                ###sqla.Column('queue',              sqla.TEXT())    : GetValue(default=None) .execution.queue                   ,#.ONLY(CodemlApplication), # exec queue _name_
                sqla.Column('time_submitted',     sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['SUBMITTED']  ,#.ONLY(CodemlApplication), # client-side submission (float) time
                sqla.Column('time_terminated',    sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['TERMINATED'] ,#.ONLY(CodemlApplication), # client-side termination (float) time
                sqla.Column('time_stopped',       sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['STOPPED']    ,#.ONLY(CodemlApplication), # client-side stop (float) time
                sqla.Column('error_tag',          sqla.TEXT())    : GetValue(default=None) .error_tag,
                sqla.Column('TRD',          sqla.TEXT())    : GetValue(default=None).TRD.ONLY((tandem_repeat_annotation_workflow.AnnotateDeNovo,tandem_repeat_annotation_workflow.AnnotateTRsFromHmmer,tandem_repeat_annotation_workflow.CalculateSignificance)),
                sqla.Column('N',          sqla.TEXT())    : GetValue(default=None).N.ONLY((tandem_repeat_annotation_workflow.AnnotateDeNovo,tandem_repeat_annotation_workflow.AnnotateTRsFromHmmer,tandem_repeat_annotation_workflow.CalculateSignificance, tandem_repeat_annotation_workflow.MergeAndBasicFilter, tandem_repeat_annotation_workflow.CalculateOverlap, tandem_repeat_annotation_workflow.RefineDenovo)),
                })

    def parse_args(self):
        self.config_file = self.params.config_file
        gc3libs.log.info("TestWorkflow config_file: {}".format(self.config_file))


    def new_tasks(self, kwargs):

        #name = "myTestWorkflow"
        gc3libs.log.info("Calling TestWorkflow.next_tasks()")

        self.config = configobj.ConfigObj(self.config_file, stringify=True)


        yield tandem_repeat_annotation_workflow.MainSequentialFlow(config = self.config, **kwargs)




######################## Support Classes / Workflow elements #############################


class MainSequentialFlow(SequentialTaskCollection):
    def __init__(self, **kwargs):

        config = kwargs["config"]
        self.kwargs = kwargs
        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format("<No parameters>"))

        self.initial_tasks = []
        if config["create_hmm_pickles"]["activated"] == 'True':
            self.initial_tasks = [DataPreparationParallelFlow(**kwargs)]
        if config["create_and_annotate_sequence_pickles"]["activated"] == 'True':
            self.initial_tasks += [SeqPreparationSequential(**kwargs)]
        else:
            self.initial_tasks += [SequencewiseParallelFlow(**kwargs)]

        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def next(self, done):
        last_task = self.tasks[done]
        rc = last_task.execution.exitcode
        gc3libs.log.info("\t MainSequentialFlow: Find next task. last_task.execution.exitcode: [%s]" % rc)
        # Stop the execution if last application failed.
        if rc != 0:
            gc3libs.log.info("\t rc is not 0 [%s]" % type(rc))
            self.execution.rc = rc
            return Run.State.STOPPED

        # Shall we continue with the workflow?
        if last_task != self.tasks[-1]:
            gc3libs.log.info("\t MainSequentialFlow: Do not add anything")
            # We still have tasks to run in self.tasks, let's consume them before adding new tasks.
            return Run.State.RUNNING
        elif isinstance(last_task, SeqPreparationSequential):
            gc3libs.log.info("\t MainSequentialFlow: Add SequencewiseParallelFlow")
            # SequencewiseParallelFlow task needs to be initialized using the output from SeqPreparationSequential
            self.add(SequencewiseParallelFlow(**self.kwargs))
            return Run.State.RUNNING
        elif isinstance(last_task, SequencewiseParallelFlow):
            gc3libs.log.info("\t MainSequentialFlow: Add SerializeAnnotations")
            self.add(SerializeAnnotations(name="serialize_annotations", **self.kwargs))
            return Run.State.RUNNING
        else:
            gc3libs.log.info("\t MainSequentialFlow: Terminated")
            # Workflow is terminated.
            self.execution.rc = rc
            return Run.State.TERMINATED


    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t MainSequentialFlow.terminated [%s]" % self.execution.returncode)


class DataPreparationParallelFlow(ParallelTaskCollection):

    def __init__(self, **kwargs):

        self.kwargs = kwargs
        gc3libs.log.info("\t\tCalling DataPreparationParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [SeqPreparationSequential(**kwargs),CreateHMMPickles(name = 'create_hmm_pickles', **kwargs)]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tDataPreparationParallelFlow.terminated")


class SeqPreparationSequential(StopOnError, SequentialTaskCollection):
    def __init__(self, **kwargs):

        gc3libs.log.info("\t\t\t\tCalling SeqPreparationSequential.__init__ ")

        config = kwargs["config"]

        if config["split_sequence_file"]["activated"] == 'True':
            self.initial_tasks = [SplitSequenceFile(name = "split_sequence_file", **kwargs),
                                CreateAnnotateSequencePickleParallelFlow(**kwargs)
                                ]
        else:
            self.initial_tasks = [CreateAnnotateSequencePickleParallelFlow(**kwargs)
                    ]

        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\t\t\tSeqPreparationSequential.terminated [%d]" % self.execution.returncode)


class CreateAnnotateSequencePickleParallelFlow(ParallelTaskCollection):

    def __init__(self, **kwargs):

        config = kwargs["config"]
        self.c = config["createannotatesequencepickle_parallel_flow"]
        self.batchsize = int(self.c['batchsize'])

        #lFile = [re.findall(self.c['retag'], i)[0] for i in os.listdir(self.c['input'])] # Extract file ID
        lFile = list(os.listdir(self.c['input']))

        self.kwargs = kwargs

        gc3libs.log.info("\t\tCalling SequencewiseParallelFlow.__init({})".format(self.kwargs))

        l = len(lFile)
        self.tasks = [CreateAnnotateSequencePickle(name = "create_and_annotate_sequence_pickles", param = {'$BATCH': " ".join(lFile[iBatch:min(iBatch+self.batchsize, l)])}, **kwargs)
                             for iBatch in range(0,l,self.batchsize)]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tSequencewiseParallelFlow.terminated")

class SequencewiseParallelFlow(ParallelTaskCollection):

    def __init__(self, **kwargs):

        config = kwargs["config"]
        self.c = config["sequencewise_parallel_flow"]

        # TODO: Find all files in dir and create self.lSeq! Warning! Should be done once the
        # Tasks before are finished.
        self.lSeq = [re.findall(self.c['retag'], i)[0] for i in os.listdir(self.c['input'])]
        self.kwargs = kwargs

        gc3libs.log.info("\t\tCalling SequencewiseParallelFlow.__init({})".format(self.kwargs))

        self.tasks = [SequenceSequential(iSeq = iSeq, **kwargs) for iSeq in self.lSeq]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tSequencewiseParallelFlow.terminated")


class SequenceSequential(StopOnError, SequentialTaskCollection):
    def __init__(self, iSeq, **kwargs):

        param = {"$N": iSeq}
        self.iSeq = iSeq
        self.initial_tasks = [TRDwiseParallelFlow(iSeq = iSeq, **kwargs),
                                MergeAndBasicFilter(name = "merge_and_basic_filter", param = param, **kwargs),
                                CalculateOverlap(name = "calculate_overlap", param = param, **kwargs),
                                RefineDenovo(name = "refine_denovo", param = param, **kwargs),
                                ]

        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\t\t\tTRDSequential.terminated [%d]" % self.execution.returncode)


class TRDwiseParallelFlow(ParallelTaskCollection):

    def __init__(self, iSeq, **kwargs):

        config = kwargs["config"]
        self.c = config["TRDwise_parallel_flow"]
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
            self.initial_tasks = [AnnotateTRsFromHmmer(name = "annotate_TRs_from_hmmer", param = self.param, **kwargs),
                                CalculateSignificance(name = "calculate_significance", param = self.param, **kwargs),
                                ]
        elif self.TRD_type == 'deNovo':
                    self.initial_tasks = [AnnotateDeNovo(name = "annotate_de_novo", param = self.param, **kwargs),
                                CalculateSignificance(name = "calculate_significance", param = self.param, **kwargs),
                                ]
        else:
            # FIXME!
            raise("TRD_type not known: {}".format(self.TRD_type))
        SequentialTaskCollection.__init__(self, self.initial_tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
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