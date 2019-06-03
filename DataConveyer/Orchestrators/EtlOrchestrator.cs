//EtlOrchestrator.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Output;
using Mavidian.DataConveyer.Routing;
using Mavidian.DataConveyer.Transform;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// Orchestrator that uses ETL processing (intake, transform, output)
   /// </summary>
   internal sealed class EtlOrchestrator : IOrchestrator
   {
      /// <summary>
      /// Outcome of an awaited SendAsync call (to post data to a dataflow block)
      /// </summary>
      private enum SendAsyncOutcome
      {
         /// <summary>
         /// Data successfully posted
         /// </summary>
         Success,
         /// <summary>
         /// Await was abandoned/canceled by errorCTkn (due to error somewhere in pipeline)
         /// </summary>
         Unblocked,
         /// <summary>
         /// Target declined data due to user cancellation (userCTkn)
         /// </summary>
         Declined
      }

      //Constants specific to progress events arguments and phase processing status
      internal const int AtStart = 0;         // ClstrCnt and RecCnt in case of PhaseStarting events
      internal const int NotApplicable = -1;  // TransfmrNo in case of Intake or Output
      private const int FirstTransfmr = 0;    // TransfmrNo in case of Transformation
      private const int TransfmrCount = 1;    // number of transformers
      //TODO: Replace tranformer numbers by "real" data (future implementation)

      private readonly OrchestratorConfig _config;
      private readonly TypeDefinitions _typeDefinitions;
      private readonly BufferBlock<KeyValCluster> _holdingBlock;                                            // Transform (2)
      private readonly Router _router;
      private readonly ActionBlock<Tuple<ExternalLine, int>> _outputBlock;                                  // Output (3)
      private readonly SingleUseBool _executionHasStartedOrDisposed;
      private readonly SingleUseBool _initializationHasCompleted;
      private readonly SingleUseBool _beforeHeadCluster;
      private readonly SingleUseBool _transformingBlockIsLinked;
      private readonly SingleUseBool _transformationHasStarted;
      private readonly SingleUseBool _unclusteringBlockIsLinked;
      private readonly SingleUseBool _outputHasStarted;
      private readonly CancellationTokenSource _userRequestedCancelSource;     //user requested cancellation
      private readonly CancellationTokenSource _pipelineErrorCancelSource;     //cancellation of operations in progress (e.g. SendAsync blocked by buffer size) in case of errors elsewhere
      private readonly CancellationTokenSource _userOrTimeoutCancelSource;     //combined cancellation (ether user requested or due to timeout)
      private SingleUseBool _pipelineErrorOccurred;                   //flag indicating that unhandled exception has caused pipeline shutdown
      private Exception _errorThatCausedShutdown;                     //the unhandled exception (likely thrown by caller supplied code) that initiated pipeline shutdown
      private SingleUseBool _recordLimitReached;                      //flag indicating that _config.IntakeRecordLimit has been reached during intake

      private readonly IGlobalCache _globalCache;
      private readonly IntakeProvider _intakeProvider;
      private readonly TransformBlock<Tuple<int, int, ExternalLine>, KeyValRecord> _lineParsingBlock;  // Intake (1)
      private readonly IPropagatorBlock<KeyValRecord, KeyValCluster> _clusteringBlock;                 // Intake (2)
      private readonly TransformProvider _transformProvider;
      private readonly TransformManyBlock<KeyValCluster, KeyValCluster> _transformingBlock;            // Transform (1)
      private readonly TransformManyBlock<KeyValCluster, KeyValRecord> _unclusteringBlock;             // Output (1)
      private readonly OutputProvider _outputProvider;
      private readonly TransformBlock<KeyValRecord, Tuple<ExternalLine, int>> _formattingBlock;        // Output (2)

      private readonly int _intakeBufferSize;
      private readonly int _transformInBufferSize;
      private readonly int _transformOutBufferSize;
      private readonly int _outputBufferSize;

      //Dataflow block links:
      private IDisposable _linkPtoC;
      private IDisposable _linkCtoT;
      private IDisposable _linkTtoH;
      private IDisposable _linkHtoU;
      private IDisposable _linkUtoF;
      private IDisposable _linkFtoO;

      //Mutating elements (e.g. running cluster/record counts per phase), needed to report in progress events and phase state:
      private PhaseStatus _inStatus;     // Intake
      private readonly PhaseStatus[] _tranStatus; // Transformation(s)
      private PhaseStatus _outStatus;    // Output

      private readonly X12Delimiters _x12DelimitersForOutput;

      /// <summary>
      /// Constructs the entire dataflow pipeline based on given configuration parameters.
      /// </summary>
      /// <param name="config"></param>
      internal EtlOrchestrator(OrchestratorConfig config)
      {
         //Remember config as settings are needed during execution
         //note that those config settings that involve some logic (e.g. _intakeSupplier) are resolved in the intake providers
         _config = config;

         //Initialize GlobalCache
         var repo = new Dictionary<string, object>();
         if (_config.GlobalCacheElements != null)
         {
            foreach(var elem in _config.GlobalCacheElements)
            {
               var tpl = ParseGlobalCacheDef(elem);
               if (!string.IsNullOrWhiteSpace(tpl.Item1)) repo.Add(tpl.Item1, tpl.Item2);
            }
         }
         _globalCache = new GlobalCache(new ConcurrentDictionary<string, object>(repo), _config.ClusterSyncInterval);


         //----
         //CTOR STEP 1: Resolve config parameters:
         //----

         _typeDefinitions = CreateTypeDefinitions(_config.ExplicitTypeDefinitions, _config.TypeDefiner);

         _x12DelimitersForOutput = new X12Delimiters { X12SegmentDelimiter = _config.DefaultX12SegmentDelimiter, X12FieldDelimiter = _config.DefaultX12FieldDelimiter };

         _intakeProvider = IntakeProvider.CreateProvider(_config, _globalCache, _typeDefinitions, _x12DelimitersForOutput);

         _transformProvider = TransformProvider.CreateProvider(_typeDefinitions, _config);

         _router = Router.CreateRouter(_config);

         _outputProvider = OutputProvider.CreateProvider(_config, _globalCache, _x12DelimitersForOutput);

         //Sizes of the respective buffers (BoundedCapacities):
         _transformInBufferSize = _config.BufferSize;
         if (_transformInBufferSize == Constants.Unlimited)
         {  //-1 (unbounded) none of the pipeline blocks are bounded
            _intakeBufferSize = DataflowBlockOptions.Unbounded;
            _transformOutBufferSize = DataflowBlockOptions.Unbounded;
            _outputBufferSize = DataflowBlockOptions.Unbounded;
         }
         else  //all blocks in the pipeline are bounded
         {
            _intakeBufferSize = Convert.ToInt32(_transformInBufferSize * _config.IntakeBufferFactor);
            _transformOutBufferSize = Convert.ToInt32(_transformInBufferSize * _config.TransformBufferFactor);
            _outputBufferSize = Convert.ToInt32(_transformInBufferSize * _config.OutputBufferFactor);
         }

         //Cancellation sources (Timeout is considered here)
         _userRequestedCancelSource = new CancellationTokenSource();
         _userOrTimeoutCancelSource = CancellationTokenSource.CreateLinkedTokenSource(_userRequestedCancelSource.Token);
         _pipelineErrorCancelSource = new CancellationTokenSource();

         //Events:
         if (_config.PhaseStartingHandler != null) this.PhaseStarting += _config.PhaseStartingHandler;
         if (_config.ProgressChangedHandler != null) this.ProgressChanged += _config.ProgressChangedHandler;
         if (_config.PhaseFinishedHandler != null) this.PhaseFinished += _config.PhaseFinishedHandler;
         if (_config.ErrorOccurredHandler != null) this.ErrorOccurred += _config.ErrorOccurredHandler;


         //----
         //CTOR STEP 2: Define dataflow blocks:
         //----

         // LineParsingBlock translates intake lines into KeyVal records (1 : 1)             - Intake (1)
         _lineParsingBlock = CreateLineParsingBlock(_intakeProvider, _userOrTimeoutCancelSource.Token);

         // ClusteringBlock translates KeyVal records to KeyVal clusters (many : 1)          - Intake (2)
         _clusteringBlock = CreateClusteringBlock(_userOrTimeoutCancelSource.Token, _pipelineErrorCancelSource.Token);

         // TransformingBlock transforms input clusters to output clusters (1 : 0..many)      - Transform (1)
         _transformingBlock = CreateTransformingBlock(_transformProvider, _userOrTimeoutCancelSource.Token);

         // HoldingBlock buffers output clusters (1 : 1)                                      - Transform (2)
         _holdingBlock = CreateHoldingBlock(_userOrTimeoutCancelSource.Token);

         // UnclusteringBlock translates clusters into KeyVal records (1 : many)              - Output (1)
         _unclusteringBlock = CreateUnclusteringBlock(_router, _userOrTimeoutCancelSource.Token);

         // FormattingBlock translates KeyVal records into output lines (1 : 1)               - Output (2)
         _formattingBlock = CreateFormattingBlock(_outputProvider, _userOrTimeoutCancelSource.Token);

         // OutputBlock sends output lines for output processing                              - Output (3)
         _outputBlock = CreateOutputBlock(_outputProvider, _userOrTimeoutCancelSource.Token);


         //----
         //CTOR STEP 3: Set starting values for processing counts (mutating state):
         //----

         _executionHasStartedOrDisposed = new SingleUseBool();
         _initializationHasCompleted = new SingleUseBool();
         _beforeHeadCluster = new SingleUseBool();
         _transformingBlockIsLinked = new SingleUseBool();
         _transformationHasStarted = new SingleUseBool();
         _unclusteringBlockIsLinked = new SingleUseBool();
         _outputHasStarted = new SingleUseBool();
         _inStatus = new PhaseStatus(Phase.Intake);
         _outStatus = new PhaseStatus(Phase.Output);
         _tranStatus = new PhaseStatus[TransfmrCount];
         for(int i = 0; i < TransfmrCount; i++) { _tranStatus[i] = new PhaseStatus(Phase.Transformation, i); }

      }  //ctor


      /// <summary>
      /// Ctor helper method to determine field type definitions from config data
      /// </summary>
      /// <param name="explicitTypeDefs"></param>
      /// <param name="typeDefiner"></param>
      /// <returns></returns>
      private TypeDefinitions CreateTypeDefinitions(string explicitTypeDefs, Func<string, ItemDef> typeDefiner)
      {
         //note that the function results are memoized inside TypeDefinitions, so any inefficiencies here are insignificant

         Func<string, ItemType> fldTypeFunc;
         Func<string, string> fldFormatFunc;

         if (typeDefiner == null)
         {
            //No TypeDefiner function in config, use default type and format definers
            fldTypeFunc = key => ItemType.String;
            fldFormatFunc = key => string.Empty;  //if format missing, empty string is assumed
         }
         else
         {
            fldTypeFunc = key => typeDefiner(key).Type;
            fldFormatFunc = key => typeDefiner(key).Format;
         }

         ConcurrentDictionary<string, ItemType> initFldTypes;
         ConcurrentDictionary<string, string> initFldFormats;
         if (explicitTypeDefs == null)
         {
            //No ExplicitTypeDefinitions in config, use empty initial set of memoized results
            initFldTypes = new ConcurrentDictionary<string, ItemType>();
            initFldFormats = new ConcurrentDictionary<string, string>();
         }
         else
         {
            var initTypeDefs = explicitTypeDefs.ParseTypeDefinitions().ToList();
            //TODO: Verify that keys are unique (i.e. check for bad config; part of "config scrubber")
            initFldTypes = new ConcurrentDictionary<string, ItemType>(initTypeDefs.ToDictionary(t => t.Item1, t => t.Item2.Type));
            initFldFormats = new ConcurrentDictionary<string, string>(initTypeDefs.ToDictionary(t => t.Item1, t => t.Item2.Format));
         }

         return new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

      }  //CreateTypeDefinitions


      /// <summary>
      /// Parse the GlobalCache element definition (key and value separated by |)
      /// </summary>
      /// <param name="cacheDef">Element definition from _config.GlobalCacheElement</param>
      /// <returns>Tuple containing the key and the value to be placed in GlobalCache</returns>
      private Tuple<string, object> ParseGlobalCacheDef(string cacheDef)
      {
         var pipeIdx = cacheDef.IndexOf("|");
         if (pipeIdx == -1) return Tuple.Create(cacheDef, (object)(null as string));  //no pipe in def value
         var key = cacheDef.SafeSubstring(0, pipeIdx++);
         var strVal = cacheDef.SafeSubstring(pipeIdx, cacheDef.Length);
         if (string.IsNullOrEmpty(strVal)) return Tuple.Create(key, (object)(string.Empty));  //empty contents after pipe
         if (strVal[0] == '"')
         { //quoted string
            var len = strVal.Length;
            return Tuple.Create(key, (object)strVal.Substring(1, strVal[len - 1] == '"' ? len - 2 : len - 1)); //closing quote removed if present
         }
         if (int.TryParse(strVal, out int intVal)) return Tuple.Create(key, (object)intVal);
         if (decimal.TryParse(strVal, out decimal decVal)) return Tuple.Create(key, (object)decVal);
         if (DateTime.TryParse(strVal, out DateTime dateVal)) return Tuple.Create(key, (object)dateVal);
         return Tuple.Create(key, (object)strVal);
      }


#region IOrchestrator implementation

      /// <summary>
      /// Outcome of initializations occurring at start of the <see cref="EtlOrchestrator.ExecuteAsync"/> method, such as opening intake sources and output targets.
      /// False denotes success.
      /// True means a failure (e.g. input file not found), in which case ExecuteAsync method will always result in CompletionStatus.InitializationError.
      /// This property should not be used before the call to the <see cref="EtlOrchestrator.ExecuteAsync"/> method. This is because it will force the initialization
      /// process, which is usually not desireable.
      /// Note that unless <see cref="OrchestratorConfig.EagerInitialization"/> setting is set to true, the initialization process stops in case of failure, so subsequent initializations are not attempted.
      /// For example, if intake initialization fails, then no output initialization is attempted.
      /// </summary>
      public bool InitErrorOccurred
      {
         get
         {
            bool intakeError = _intakeProvider.InitErrorOccurred;
            bool outputError = !intakeError || _config.EagerInitialization ? _outputProvider.InitErrorOccurred : false;
            return intakeError || outputError;
         }
      }


      /// <summary>
      /// Processes the entire pipeline. This method is intended for single use as it disposes some resources (such as input/output files) acquired by the constructor.
      /// </summary>
      /// <returns>A task containing processing results</returns>
      public async Task<ProcessResult> ExecuteAsync()
      {
         //Verify if orchestrator is waiting to start execution
         if (!_executionHasStartedOrDisposed.FirstUse)
         {
            //Execution has been attempted (or Dispose method was called) before; exit immediately (after logging fatal error)
            _config.Logger.LogFatal("Invalid attempt to start execution (either subsequent attempt or orchestrator disposed).");
            return new ProcessResult(CompletionStatus.InvalidAttempt, 0, 0, 0, 0, _globalCache);
         }

         //Verify if problems occurred in constructing the orchestrator
         if (InitErrorOccurred)
         {
            //Orchestrator was not created properly (e.g. input file not found); exit immediately
            return new ProcessResult(CompletionStatus.InitializationError, 0, 0, 0, 0, _globalCache);
         }

         var dummy = _initializationHasCompleted.FirstUse;  // "touch" _initializationHasCompleted so that FirstUse will return false thereafter

         _pipelineErrorOccurred = new SingleUseBool();
         _recordLimitReached = new SingleUseBool();

         //Link blocks together:
         var propCompletion = new DataflowLinkOptions { PropagateCompletion = true };  //TODO: consider other options, e.g. SingleProducerConstrained
         _linkPtoC = _lineParsingBlock.LinkTo(_clusteringBlock, propCompletion);
         //ReadyForTransform links clustering to transforming blocks, but its timing is controlled by the DeferTransformation setting
         if (_config.DeferTransformation == DeferTransformation.NotDeferred) ReadyForTransform();
         _linkTtoH = _transformingBlock.LinkTo(_holdingBlock, propCompletion);
         //In general, UnclusteringBlock cannot be started (linked to) when DataConveyer starts. This is because needed data (such as FieldsToUse in FlatOutputProvider) may not yet be known.
         // So instead, ReadyForOutput method (which in turn links HoldingBlock to UnclusteringBlock) is called when all needed data is set.
         //RFO#1 (start of pipeline)
         if (OutputCanStartAt == OutputStartTiming.StartOfPipeline)
         {
            ReadyForOutput(null);
         }
         _linkUtoF = _unclusteringBlock.LinkTo(_formattingBlock, propCompletion);
         _linkFtoO = _formattingBlock.LinkTo(_outputBlock, propCompletion);

         //Ready to launch the process, start timeout counter if applicable
         if (_config.TimeLimit != Timeout.InfiniteTimeSpan) _userOrTimeoutCancelSource.CancelAfter(_config.TimeLimit);
         _config.Logger.LogInfo("Data Conveyer process starting...");

         SafelyUpdateStatus(ref _inStatus, s => s.UpdateRunningCounts(s.RecordCount, s.ClusterCount));  //change intake PhaseState from NotYetStarted to Running (both counts should be AtStart when UpdateRunningCounts executes)

         //Intake PhaseStarting event:
         _config.Logger.LogInfo("Intake phase starting...");
         if (_config.ReportProgress) InvokePhaseStarting(new PhaseEventArgs(Phase.Intake, AtStart, AtStart, NotApplicable, _globalCache));

         //Iterate over intake lines and post them onto the line parsing block
         try
         {
            await FeedPipelineAsync(_userOrTimeoutCancelSource.Token, _pipelineErrorCancelSource.Token);
         }
         catch { /* do nothing, i.e. "swallow" exception */ } //this exception indicates cancellations or fault, in either case _lineParsingBlock will be faulted/canceled as well, which will propagate to _outputBlock 
         finally
         {
            _intakeProvider.DisposeIntake();
         }

         try
         {
            await _lineParsingBlock.Completion;
         }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }

         if (_config.DeferTransformation == DeferTransformation.UntilRecordInitiation) ReadyForTransform();  //it's only needed if none of the RecordInitiators returned true, but it can't hurt to call it regardless

         //Let's start formatting if no more fields will be added
         //RFO#3 (LineParsing complete)
         if (!_config.AllowTransformToAlterFields)  // equivalent to: if (OutputCanStartAt <= OutputStartTiming.LineParsingCompleted)
         {
            ReadyForOutput(_intakeProvider.FieldsInUse);
            _transformProvider.SetFieldsInUse(_intakeProvider.FieldsInUse.ToList());
         }

         try { await _clusteringBlock.Completion; }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }

         //Intake PhaseFinished event:
         SafelyUpdateStatus(ref _inStatus, s => s.SetComplete());  //change PhaseState from Running to Complete
         _config.Logger.LogInfo("Intake phase finished.");
         if (_config.ReportProgress) InvokePhaseFinished(new PhaseEventArgs(Phase.Intake, _inStatus.ClusterCount, _config.InputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstInputRow ? _intakeProvider._lineCnt - 1 : _intakeProvider._lineCnt, NotApplicable, _globalCache));

         if (_config.DeferTransformation == DeferTransformation.UntilIntakeCompletion) ReadyForTransform();

         try { await _transformingBlock.Completion; }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }

         //At this point, we know for sure no more fields will be added, so we're ready to call ReadyForOutput (RFO#4). But let's report transformation completion
         // first, to prevent reporting Output start (reported by unclustering block upon being linked, which is done by ReadyForOutput) before reporting
         // Transformation finish in case of RF#4.

         //Transformation PhaseFinished event:
         if (!_transformationHasStarted.FirstUse)  //don't report if transformation not yet started, e.g. shutdown due to error in intake
         {
            SafelyUpdateStatus(ref _tranStatus[FirstTransfmr], s => s.SetComplete());  //change PhaseState from Running to Complete
            _config.Logger.LogInfo("Transformation phase finished.");
            if (_config.ReportProgress)
            {
               var tranStatus = _tranStatus[FirstTransfmr];
               InvokePhaseFinished(new PhaseEventArgs(Phase.Transformation, tranStatus.ClusterCount, tranStatus.RecordCount, FirstTransfmr, _globalCache));
            }
         }

         //RFO#4 (Transformation complete)
         ReadyForOutput(_transformProvider.FieldsInUse);  //chances are, FieldsInUse have been assigned before, but it's OK (they won't be overwritten in such case)

         try { await _holdingBlock.Completion; }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }

         //TODO: Consider removing awaiting unclustering and formatting blocks.
         try { await _unclusteringBlock.Completion; }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }
         try { await _formattingBlock.Completion; }
         catch { /* do nothing, i.e. "swallow" exception and let cancellations/faults automatically propagate to _outputBlock */ }

         Task pipelineCompletionTask = _outputBlock.Completion;  //note that the completion task is done once output buffer gets emptied
         try
         {
            await pipelineCompletionTask;
         }
         catch
         {
            //We are "swallowing" the exception one final time; the result to return will be determined based on the pipelineCompletionTask.Status, etc.
         }

         //Pipeline has finished, we are almost done (but a trailer may need to be sent to output)
         if (pipelineCompletionTask.Status == TaskStatus.RanToCompletion)
         {
            //Send end-of-data mark (possibly preceded by trailer lines)
            try  //we still can experience exception from any of the SendLineToOutput.. calls below
            {
               if (_config.AsyncOutput)
               {
                  await _outputProvider.SendLineToOutputAsync(null);  // end-of-data (will send trailers as appropriate)
               }
               else  //output is synchronous
               {
                  _outputProvider.SendLineToOutput(null);  // end-of-data (will send trailers as appropriate)
               }
            }
            catch (Exception ex)  //by all likelihood, exception thrown by a caller-supplied OutputConsumer function
            {
               //This is a special case, pipeline is gone, we need to wrap things up to be as clean as possible
               _config.Logger.LogFatal("An error occurred while sending trailer to output.", ex);    //equivalent to "initiating shutdown.."
               InvokeErrorOccurred(new ErrorEventArgs("trailer output", string.Empty, ex, _globalCache));
               //Output PhaseFinished event:
               // note that we don't need to verify _outputHasStarted here, it's guaranteed to have started if pipeline completed successfully.
               SafelyUpdateStatus(ref _outStatus, s => s.SetComplete());  //change PhaseState from Running to Complete
               _config.Logger.LogInfo("Output phase finished.");
               if (_config.ReportProgress) InvokePhaseFinished(new PhaseEventArgs(Phase.Output, _outStatus.ClusterCount, _outputProvider.WrittenDataLinesCnt, NotApplicable, _globalCache));
               // note that PhaseFinished event does not report Header/Leader/Trailer output rows (i.e. WrittenDataLinesCnt excludes them; they are included WrittenLinesTotalCount though)
               _config.Logger.LogInfo("Data Conveyer process failed due to an error in sending trailer to output.");
               return new ProcessResult(CompletionStatus.Failed, _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
            }
            finally
            {
               _outputProvider.DisposeOutput();
            }
         }
         else  //did not RanToCompletion
         {
            _outputProvider.DisposeOutput();
         }

         //Output PhaseFinished event:
         if (!_outputHasStarted.FirstUse)  //don't report if output not yet started, e.g. shutdown due to error in intake
         {
            SafelyUpdateStatus(ref _outStatus, s => s.SetComplete());  //change PhaseState from Running to Complete
            _config.Logger.LogInfo("Output phase finished.");
            if (_config.ReportProgress) InvokePhaseFinished(new PhaseEventArgs(Phase.Output, _outStatus.ClusterCount, _outputProvider.WrittenDataLinesCnt, NotApplicable, _globalCache));
            // note that PhaseFinished event does not report Header/Leader/Trailer output rows (i.e. WrittenDataLinesCnt excludes them; they are included in WrittenLinesTotalCount though)
         }

         //Truly done here, let's evaluate final status and exit
         switch (pipelineCompletionTask.Status)
         {
            case TaskStatus.Canceled:
               if (_pipelineErrorOccurred.FirstUse)
               {  //No error occurred, so it can either be user canceled or timeout
                  if (_userRequestedCancelSource.IsCancellationRequested)
                  {
                     _config.Logger.LogInfo("Data Conveyer process was canceled.");
                     return new ProcessResult(CompletionStatus.Canceled, _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
                  }
                  else
                  {
                     _config.Logger.LogInfo("Data Conveyer process timed out.");
                     return new ProcessResult(CompletionStatus.TimedOut, _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
                  }
               }
               else
               {  //Error occurred, which in turn initiated shutdown via _pipelineErrorCancelSource (it was propagated as Cancellation and not Fault)
                  _config.Logger.LogInfo($"Data Conveyer process failed due to an error in {_errorThatCausedShutdown.Source}.");  //Source of the originally thrown exception is expected to be name of the offending block
                  return new ProcessResult(CompletionStatus.Failed, _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
               }
            case TaskStatus.Faulted:  //here, error occurred in last block (or fault was propagated literally - unlikely)
               var ex = pipelineCompletionTask.Exception.Flatten().InnerException;
               Debug.Assert(ex == _errorThatCausedShutdown);
               _config.Logger.LogInfo($"Data Conveyer process failed due to an error in {ex.Source}.");  //Source of the originally thrown exception is expected to be name of the offending block
               return new ProcessResult(CompletionStatus.Failed, _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
            default:  //since task has been awaited for, the only status left here is RanToCompletion
               _config.Logger.LogInfo("Data Conveyer process finished successfully.");
               return new ProcessResult(_recordLimitReached.FirstUse ? CompletionStatus.IntakeDepleted : CompletionStatus.LimitReached,
                                        _intakeProvider._lineCnt, _inStatus.ClusterCount, _outStatus.ClusterCount, _outputProvider.WrittenLinesTotalCnt, _globalCache);
               //                       Note that _recordLimitReached.FirstUse is true when the FirstUse property is 
               //                       called for the first time, i.e. the limit has not been reached
         }

      }  //ExecuteAsync


      /// <summary>
      /// Cancels execution in progress, if any.
      /// </summary>
      public void CancelExecution()
      {
         try
         {
            _userRequestedCancelSource.Cancel();
         }
         catch (ObjectDisposedException) { }  //"The CancellationTokenSource has been disposed." - ignore it
      }

      //Events:
      /// <summary>
      /// Occurs upon start of a pipeline phase
      /// </summary>
      private event EventHandler<PhaseEventArgs> PhaseStarting;
      /// <summary>
      /// Occurs when a pipeline phase completes
      /// </summary>
      private event EventHandler<PhaseEventArgs> PhaseFinished;
      /// <summary>
      /// Occurs when progress of processing pipeline phase reaches specified interval
      /// </summary>
      private event EventHandler<ProgressEventArgs> ProgressChanged;
      /// <summary>
      /// Occurs when a process shutdown is initiated due to unhandled exception thrown, for example by the caller supplied code. Subsequently, a completion status will be Failed.
      /// </summary>
      private event EventHandler<ErrorEventArgs> ErrorOccurred;


      /// <summary>
      /// Release resources that may be held after process execution. This method should be called after completion of <see cref="EtlOrchestrator.ExecuteAsync"/> method.
      /// </summary>
      public void Dispose()
      {
         //Verify if processing is underway
         if (!_executionHasStartedOrDisposed.FirstUse &&  //note that calling _executionHasStartedOrDisposed.FirstUse prevents subsequent start of processing
             !_initializationHasCompleted.FirstUse &&
             !_outputBlock.Completion.IsCompleted)  
         {
            //NOTE: Alternatively, the process could be aborted here (e.g. InitiateShutdown), but it adds complexities (e.g. need to wait for completion before proceeding to release resources)
            _config.Logger.LogWarning("Dispose called while processing, resources not released (next time consider canceling execution or awaiting completion instead).");
            return;
         }

         //No need to implement the MS disposable pattern: #1 - no unmanaged resources held, #2 - the class is sealed
         //Instead, we are simply disposing those managed objects held that implement IDisposable

         //Cancellation tokens:
         _userRequestedCancelSource.Dispose();
         _pipelineErrorCancelSource.Dispose();
         _userOrTimeoutCancelSource.Dispose();

         //Unlink dataflow blocks (note that the links may be null, e.g. if init failure, cancel, etc.):
         _linkPtoC?.Dispose();
         _linkCtoT?.Dispose();
         _linkTtoH?.Dispose();
         _linkHtoU?.Dispose();
         _linkUtoF?.Dispose();
         _linkFtoO?.Dispose();

         //File reader and writer should have been disposed within ExecuteAsync, but it can't hurt to do it again:
         _intakeProvider.DisposeIntake();
         _outputProvider.DisposeOutput();

         //Unsubscribe events:
         //EtlOrchestrator is publisher here (it raises events, which are subscribed elsewhere like _config below).
         //So, by unsubscribing, we are allowing _config to be GC'd right away (before EtlOrchestrator goes out of scope).
         //Note that these events can also be subscribed to directly in client code, in which case they need to be unsubscribed
         // manually.
         //This doesn't save much, but can't hurt either (would've been necessary if EtlOrchestrator was subscribed to event(s)
         // raised by other objects that was held alive after EtlOrchestrator disposal).
         if (_config.PhaseStartingHandler != null) this.PhaseStarting -= _config.PhaseStartingHandler;
         if (_config.ProgressChangedHandler != null) this.ProgressChanged -= _config.ProgressChangedHandler;
         if (_config.PhaseFinishedHandler != null) this.PhaseFinished -= _config.PhaseFinishedHandler;
         if (_config.ErrorOccurredHandler != null) this.ErrorOccurred -= _config.ErrorOccurredHandler;

         if (_config.CloseLoggerOnDispose)
         {
            _config.Logger.LogEnd();
            _config.Logger.Dispose();
         }
      }

#endregion IOrchestrator implementation


#region Private properties

      /// <summary>
      /// Timing at which Output can start processing.
      /// Determination is made based solely on configuration settings.
      /// </summary>
      /// <returns>One of OutputStartTiming values</returns>
      private OutputStartTiming OutputCanStartAt
      {
         get
         {
            //Q1: Do we know what the output fields are?
            if (!string.IsNullOrWhiteSpace(_config.OutputFields)) return OutputStartTiming.StartOfPipeline;  //RFO#1

            //Q2: Do we need to know the output fields up-front?
            if (!_config.OutputDataKind.OutputFieldsAreNeededUpFront()) return OutputStartTiming.StartOfPipeline;  //RFO#1

            //Here, we know that output requires fields up-front and the fields aren't known at start of pipeline
            // (specifically the FormattingBlock needs FieldsToUse up-front)

            //Q3: Will fields be subject to change during transformation?
            if (_config.AllowTransformToAlterFields) return OutputStartTiming.TransformCompleted;  //RFO#4

            //Here, all fields will be determined during Intake (either after 1st row or at end of line parsing)

            //Q4: Can fields be changed beyond the 1st row on intake?
            if (_config.InputDataKind.OnTheFlyInputFieldsCanBeAllowed() && _config.AllowOnTheFlyInputFields) return OutputStartTiming.LineParsingCompleted;  //RFO#3

            //Here, we know that all fields are known after the 1st row
            return OutputStartTiming.HeaderRowProcessed;  //RFO#2
         }
      }

#endregion Private properties


#region Private methods

      /// <summary>
      /// Asynchronously read intake lines and post them onto the line parsing block (i.e. start of the pipeline) 
      /// </summary>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <param name="errorCTkn">Cancellation token to facilitate cancellation due to an error somewhere in the pipeline</param>
      /// <returns>Task that will provide number of records processed</returns>
      private Task<int> FeedPipelineAsync(CancellationToken userCTkn, CancellationToken errorCTkn)
      {
         //Note that this is an async method. We do not need async modifier or to await inner tasks as in both cases
         //they're tail calls, i.e. there's no continuation to run. Hence, we simply return these tasks.
         if (_config.AsyncIntake)
         {
            //We already have asynchronous intake supplier, simply use it 
            return FeedPipelineFromIntakeAsync(userCTkn, errorCTkn);
         }

         //Here, we will be using synchronous intake consumer, lets wrap it in a task
         return Task.Run(() => FeedPipelineFromIntake(userCTkn, errorCTkn));
         //TODO: Consider Task.Factory.StartNew instead of Task.Run to refine the synchronous intake process

         //NOTE: Exceptions, such as OperationCanceledException thrown in FeedPipelineFromIntake (note another thread) will be
         //      caught in the calling method, i.e. ExecuteAsync. However, during debugging (and only when "Just My Code"
         //      is enabled), VS may in some cases break as if the exception was not handled in user code. According to:
         //      https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/exception-handling-task-parallel-library
         //      , it is benign and hitting F5 will resume execution appropriately. This VS behavior can be prevented by
         //      unchecking "Enable Just My Code" under Tools..Options..Debugging..General.
      }


      /// <summary>
      /// Read input lines from intake and post them onto the line parsing block (i.e. start of the pipeline)
      /// </summary>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <param name="errorCTkn">Cancellation token to facilitate cancellation due to an error somewhere in the pipeline</param>
      /// <returns>Number of intake records processed</returns>
      private int FeedPipelineFromIntake(CancellationToken userCTkn, CancellationToken errorCTkn)
      {
         Tuple<int, int, ExternalLine> currentLine = null;  //tuple consisting of seq#, sourceNo and the line
         Exception exCaught = null;

         try
         {
            currentLine = _intakeProvider.GetLineFromIntake();
         }
         catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function (or by XML/JSON parser)
         {
            string origin = _config.InputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstInputRow ? "header intake" : "intake block";
            InitiateShutdown(ex, origin, $" after line #{_intakeProvider._lineCnt.ToString()}");
            exCaught = ex;
         }
         userCTkn.ThrowIfCancellationRequested();

         if (exCaught == null)
         {
            // here, in case of X12, currentLine is (presumably) the ISA segment; note that the field delimiter is determined in FieldTokenizer of X12IntakeProvider

            //Check if 1st line contains headers (except for KW or arbitrary data)
            if (_config.InputDataKind.CanHaveHeaderRow() &&
                _config.HeadersInFirstInputRow &&
                currentLine != null)  //make sure 1st line is supplied (e.g. DefaultIntakeSupplier supplies just the null line, i.e. EOF)
            {
               //read first line to determine field headers (aka field names aka keys)
               _intakeProvider.IncludeFieldsEnMasse(_intakeProvider.FieldTokenizer(currentLine.Item3));
               try
               {
                  currentLine = _intakeProvider.GetLineFromIntake();
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
               {
                  InitiateShutdown(ex, "intake block", $" after line #{_intakeProvider._lineCnt.ToString()}");
                  exCaught = ex;
               }
               userCTkn.ThrowIfCancellationRequested();
            }
         }

         _intakeProvider.HeaderRowIsComplete();  //to possibly disallow additions of new fields on the fly

         //Let's start output (formatting) if no more fields will be added
         //RFO#2 (HeadearRow complete)
         if (OutputCanStartAt <= OutputStartTiming.HeaderRowProcessed)
         //note <= and not == above; ReadyForOutput needs to be called even if output started at StartOfPipeline (to pass FieldsInUse)
         //                          the same comment applies (even more importantly) to OutputStartTiming.LineParsingCompleted, i.e. RFO#3 in ExecuteAsync method
         {
            ReadyForOutput(_intakeProvider.FieldsInUse);
         }

         //Data lines start here:
         while (currentLine != null && exCaught == null)
         {
            try
            {
               _lineParsingBlock.SendAsync(currentLine, errorCTkn).Wait();
            }
            catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)  //canceled by errorCTkn
            {
               //Error occurred somewhere in pipeline, do nothing at this point.
               //We can't break here, as it would incorrectly complete _lineParsingBlock, instead of faulting it.
               //We could fault line parsing block here, but this is expected to be done by the block that actually faulted.
               throw _errorThatCausedShutdown;  //so that the awaited FeedPipelineAsync (in ExecuteAsync) completes in Faulted state
            }

            //Note that instead of a simple ...SendAsync(...).Wait() above, we could've had something like:
            //     var sendTask = _lineParsingBlock.SendAsync(currentLine, errorCTkn);
            //     bool sendOK = false; //true = accepted, false = declined
            //     try {  sendOK = sendTask.Result; } catch { ... } 
            // , which would allow determination if the currentLine was accepted by line parsing block (or declined due to user requested cancellation: 
            //     if (!sendOK) ...  //_lineParsingBlock declined the posted line as it was canceled by user
            //This logic (see also enum SendAsyncOutcome) is not needed as it only applies to cases where pipeline is shut down.

            if (_config.IntakeRecordLimit == Constants.Unlimited || currentLine.Item1 < _config.IntakeRecordLimit)
            {
               try
               {
                  currentLine = _intakeProvider.GetLineFromIntake();
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
               {
                  InitiateShutdown(ex, "intake block", $" after line #{_intakeProvider._lineCnt.ToString()}");
                  exCaught = ex;
               }
            }
            else
            {  //max record count reached
               _ = _recordLimitReached.FirstUse;  // "touch" the _recordLimitReached so that FirstUse will return false thereafter
               currentLine = null;
            }
            userCTkn.ThrowIfCancellationRequested();
         }

         //No more input
         if (exCaught == null)
         {
            _lineParsingBlock.Complete();
         }
         else
         {
            ((IDataflowBlock)_lineParsingBlock).Fault(exCaught);
            throw exCaught;  //so that the awaited FeedPipelineAsync (in ExecuteAsync) completes in Faulted state
         }

         return _intakeProvider._lineCnt;
      }


      /// <summary>
      /// Asynchronously read input lines from intake and post them onto the line parsing block (i.e. start of the pipeline)
      /// </summary>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <param name="errorCTkn">Cancellation token to facilitate cancellation due to an error somewhere in the pipeline</param>
      /// <returns>Task with the number of intake records processed</returns>
      private async Task<int> FeedPipelineFromIntakeAsync(CancellationToken userCTkn, CancellationToken errorCTkn)
      {
         Tuple<int, int, ExternalLine> currentLine = null;  //tuple consisting of seq# (1-based), sourceNo (1-based) and the line text
         Exception exCaught = null;

         try
         {
            currentLine = await _intakeProvider.GetLineFromIntakeAsync();
         }
         catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
         {
            string origin = _config.InputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstInputRow ? "header intake" : "intake block";
            InitiateShutdown(ex, origin, $" after line #{_intakeProvider._lineCnt.ToString()}");
            exCaught = ex;
         }
         userCTkn.ThrowIfCancellationRequested();

         if (exCaught == null)
         {
            //Check if 1st line contains headers (except for kinds that can't have headers)
            if (_config.InputDataKind.CanHaveHeaderRow() &&
                _config.HeadersInFirstInputRow &&
                currentLine != null)  //make sure 1st line is supplied (e.g. DefaultIntakeSupplier supplies just the null line, i.e. EOF)
            {
               //read first line to determine field headers (aka field names aka keys)
               _intakeProvider.IncludeFieldsEnMasse(_intakeProvider.FieldTokenizer(currentLine.Item3));
               try
               {
                  currentLine = await _intakeProvider.GetLineFromIntakeAsync();
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
               {
                  InitiateShutdown(ex, "intake block", $" after line #{_intakeProvider._lineCnt.ToString()}");
                  exCaught = ex;
               }
               userCTkn.ThrowIfCancellationRequested();
            }
         }

         _intakeProvider.HeaderRowIsComplete();  //to possibly disallow additions of new fields on the fly

         //Let's start formatting if no more fields will be added
         //RFO#2 (HeadearRow complete)
         if (OutputCanStartAt <= OutputStartTiming.HeaderRowProcessed)
         //note <= and not == above; ReadyForOutput needs to be called even output started at StartOfPipeline (to pass FieldsInUse)
         //                          the same comment applies (even more importantly) to OutputStartTiming.LineParsingCompleted, i.e. RFO#3 in ExecuteAsync method
         {
            ReadyForOutput(_intakeProvider.FieldsInUse);
         }


         //Data lines start here:
         while (currentLine != null && exCaught == null)
         {
            try
            {
               await _lineParsingBlock.SendAsync(currentLine, errorCTkn);
            }
            catch (OperationCanceledException)
            {
               //error occurred somewhere in pipeline
               throw _errorThatCausedShutdown;  //so that the awaited FeedPipelineAsync (in ExecuteAsync) completes in Faulted state
            }

            if (_config.IntakeRecordLimit == Constants.Unlimited || currentLine.Item1 < _config.IntakeRecordLimit)
            {
               try
               {
                  currentLine = await _intakeProvider.GetLineFromIntakeAsync();
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
               {
                  InitiateShutdown(ex, "intake block", $" after line #{_intakeProvider._lineCnt.ToString()}");
                  exCaught = ex;
               }
            }
            else
            {  //max record count reached
               _ = _recordLimitReached.FirstUse;  // "touch" the _recordLimitReached so that FirstUse will return false thereafter
               currentLine = null;
            }
            userCTkn.ThrowIfCancellationRequested();
         }

         //No more input
         if (exCaught == null)
         {
            _lineParsingBlock.Complete();
         }
         else
         {
            ((IDataflowBlock)_lineParsingBlock).Fault(exCaught);
            throw exCaught;  //so that the awaited FeedPipelineAsync (in ExecuteAsync) completes in Faulted state
         }

         return _intakeProvider._lineCnt;
      }


      /// <summary>
      /// Create the line parsing block, a TransformBlock that translates input line tuples (each containing seq#, sourceNo and line) into KeyVal records.
      /// </summary>
      /// <param name="intakeProvider">Supplies: either 2 functions that are specific to the input data type (and match each other) - FieldTokenizer and ItemFromToken; or ItemFromExtItem function.</param>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation.</param>
      /// <returns>A transform block that builds KeyVal records from text lines></returns>
      private TransformBlock<Tuple<int, int, ExternalLine>, KeyValRecord> CreateLineParsingBlock(IntakeProvider intakeProvider, CancellationToken userCTkn)
      {
         Func<ExternalLine, IEnumerable<string>> fieldTokenizer = intakeProvider.FieldTokenizer;  // A function that takes input line (e.g. string) and returns set of tokens (string[])
         Func<string, int, IItem> itemFromToken = intakeProvider.ItemFromToken;                   // A matching function that takes a token (string), field# and returns an item (KeyValItem)
         Func<Tuple<string, object>, IItem> itemFromExtItem = intakeProvider.ItemFromExtItem;     // Replaces FieldTokenizer/ItemFromToken in case of XrecordIntakeProvider

         var settableTraceBin = new Dictionary<string, object>();

         //Helper function to construct the KeyValRecord from text line (delimited, flat, keyword, etc.):
         KeyValRecord textLineParser(Tuple<int, int, ExternalLine> tpl)
         {
            //Take input line (text), parse it using fieldTokenizer function, use itemFromToken function to construct KeyValRecord to be returned
            // tpl is a tuple consisting of line seq#, sourceNo and the line contents (of Xtext type)
            var textLine = tpl.Item3;
            Debug.Assert(textLine.Type == ExternalLineType.Xtext);
            int recNo = _config.InputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstInputRow ? tpl.Item1 - 1 : tpl.Item1;  //make sure RecNo values start at 1 (i.e. ignore header row if any)
            int i = 0;  //item index
            IEnumerable<string> tokens;
            try { tokens = fieldTokenizer(textLine); }
            catch (Exception ex) //e.g. ArgumentException thrown by X12IntakeProvider.FieldTokenizer if custom intake supplier provided too short ISA segment
            {
               InitiateShutdown(ex, "tokenizer in line parser", $" at line #{tpl.Item1.ToString()}");
               throw;
            }
            return new KeyValRecord(tokens.SelectMany(token =>
            {
               IItem itm;
               try
               {
                  itm = itemFromToken(token, i++);  //null if item to be excluded (e.g. mismatched prefix on KW data)
                                                    //return 0 or 1 items (0, i.e. empty list if item was excluded)
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function (TypeDefiner)
               {
                  InitiateShutdown(ex, "line parsing block", $" at line #{tpl.Item1.ToString()}");
                  throw;
               }
               return (itm == null) ? Enumerable.Empty<IItem>() : new IItem[] { itm };
            }), recNo, tpl.Item2, 0, _globalCache, null, null, _typeDefinitions, _config, ProcessingStatusSupplier, _config.ActionOnDuplicateKey);
         }

         //Helper function to construct the record from a record-like line (e.g. XML):
         KeyValRecord recordLineParser(Tuple<int, int, ExternalLine> tpl)
         {
            //Take input line (Xrecord), use its Items collection to construct KeyValRecord to be returned (no  fieldTokenizer or itemFromToken functions are used)
            // tpl is a tuple consisting of line seq#, sourceNo and the line contents (of Xrecord type)
            var recordLine = tpl.Item3;
            Debug.Assert(recordLine.Type == ExternalLineType.Xrecord);
            return new KeyValRecord(recordLine.Items.SelectMany(item =>
            {
               IItem itm;
               try
               {
                  itm = itemFromExtItem(item);  //null if item to be excluded (e.g. mismatched prefix on KW data)
                                                //return 0 or 1 items (0, i.e. empty list if item was excluded)
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function (TypeDefiner)
               {
                  InitiateShutdown(ex, "line parsing block", $" at record #{tpl.Item1.ToString()}");
                  throw;
               }
               return (itm == null) ? Enumerable.Empty<IItem>() : new IItem[] { itm };
            }), tpl.Item1, tpl.Item2, tpl.Item3.ClstrNo, _globalCache, null, null, _typeDefinitions, _config, ProcessingStatusSupplier, _config.ActionOnDuplicateKey);

         }

         //Note similarities between textLineParser and recordLineParser: their signatures are the same and the structures almost identical.
         // However, it's difficult to merge them together due to lack of discriminated unions in C# - note different types in arguments to
         // respective SelectMany functions and corresponding differences between itemFromToken and itemFromExtItem.

         //The line parsing block to be returned:
         var lpBlock = new TransformBlock<Tuple<int, int, ExternalLine>, KeyValRecord>(tpl =>
         {

            KeyValRecord recToReturn;
            switch (_config.InputDataKind.ExternalLineType())
            {
               case ExternalLineType.Xtext: recToReturn = textLineParser(tpl); break;
               case ExternalLineType.Xrecord: recToReturn = recordLineParser(tpl); break;
               case ExternalLineType.Xsegment: recToReturn = textLineParser(tpl); break;  //TODO: A separate parser for Xsegment
               default: throw new NotSupportedException($"No formula to parse {_config.InputDataKind} data kind.");
            }

            //RecordInitiator has 2 objectives: set the trace bin and start transform (if DeferTransformation.UntilRecordInitiation).
            //RecordInitiator is called only when custom RecordInitiator is present (performance).
            //RecordInitiator's return value is respected only if DeferTransformation is set to UntilRecordInitiation.
            // (note that if DeferTransformation.T UntilRecordInitiation, but no custom initiator, then transform starts after the 1st record).
            bool releaseTransform;
            if (_config.RecordInitiator == OrchestratorConfig.DefaultRecordInitiator)
            {  //no RecordInitiator provided; don't bother calling default RecordInitiator
               releaseTransform = _config.DeferTransformation == DeferTransformation.UntilRecordInitiation;
            }
            else
            {  //Custom RecordIniitator is called regardless of DeferTransformation setting, but releaseTransform
               // can only be set in case of DeferTransformation.UntilRecordInitiation:
               try
               {
                  releaseTransform = _config.RecordInitiator(new ReadOnlyRecord(recToReturn, true), settableTraceBin);
               }
               catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function (TypeDefiner)
               {
                  InitiateShutdown(ex, "initiating record", $" at line #{tpl.Item1.ToString()}");
                  throw;
               }
               releaseTransform = releaseTransform && _config.DeferTransformation == DeferTransformation.UntilRecordInitiation;
               //Set the trace bin for use in the subsequent records, but only if it holds some data
               if (settableTraceBin.Any()) recToReturn.SetTraceBin(settableTraceBin);  //note that SetTraceBin creates a clone, so that the settableTraceBin can mutate
               //Start transformation phase if ready and needed              
            }
            if (releaseTransform) ReadyForTransform();
            return recToReturn;
         }, new ExecutionDataflowBlockOptions
         {
            BoundedCapacity = _intakeBufferSize,
            SingleProducerConstrained = true,
            CancellationToken = userCTkn
         });

         //We're at the start of pipeline; fault will propagate from here, no need for OnlyOnFaulted continuation

         return lpBlock;
      }


      /// <summary>
      /// Create the clustering block, a custom "BufferBlock" ("dynamic" batch block) that batches records based on a function to mark begin/end of clusters
      /// </summary>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <param name="errorCTkn">Cancellation token to facilitate cancellation due to an error somewhere in the pipeline</param>
      /// 
      /// <returns>A custom propagator block that batches records into clusters</returns>
      private IPropagatorBlock<KeyValRecord, KeyValCluster> CreateClusteringBlock(CancellationToken userCTkn, CancellationToken errorCTkn)
      {
         //"Dynamic" batch block, a custom propagator block
         //inspired by: https://social.msdn.microsoft.com/Forums/en-US/cb3df92b-e991-437c-bf7f-824b4c23df91/dynamicbatchblock?forum=tpldataflow
         var recsInProgress = new List<KeyValRecord>();
         int countInProgress = 0; //same as recsInProgress.Count
         int lastRecNo = 0;       //may be needed in foot cluster
         KeyValRecord previousRec = null;

         Func<KeyValRecord, KeyValRecord, int, bool> marker = _config.ClusterMarker;
         bool markerStartsCluster = _config.MarkerStartsCluster;

         var outBlock = new BufferBlock<KeyValCluster>(new DataflowBlockOptions
         {
            BoundedCapacity = _transformInBufferSize,
            CancellationToken = userCTkn
         });
         var inBlock = new ActionBlock<KeyValRecord>(async rec =>  //cumulate records until predicate condition met
                                                      {
                                                         if (!markerStartsCluster)
                                                         {
                                                            recsInProgress.Add(rec);
                                                            countInProgress++;
                                                         }

                                                         bool clusterIsComplete;

                                                         try
                                                         {
                                                            clusterIsComplete = marker(rec, previousRec, countInProgress) && countInProgress > 0;
                                                         }
                                                         catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function (ClusterMarker)
                                                         {
                                                            InitiateShutdown(ex, "clustering block", $" at record #{rec.RecNo.ToString()}");
                                                            throw;
                                                         }

                                                         if (clusterIsComplete)
                                                         {  //cluster complete
                                                            await PostClusterAsync(countInProgress, recsInProgress, outBlock, errorCTkn, 0);
                                                            //It's possible that the cluster will not be posted to outBlock: canceled due to errorCTkn or outBlock declined as a result of userCTkn. This
                                                            // is reported in the return value, which is of SendAsyncOutcome type.
                                                            //In these cases the pipeline is being shut down, so we are letting things taking its own course (e.g. the fault will be received from _lineParsingBlock)
                                                            //TODO: Consider direct action to fault inBlock when SendAsyncOutcome.Unblocked is returned (throw?)
                                                            lastRecNo = recsInProgress.Last().RecNo;
                                                            recsInProgress.Clear();
                                                            countInProgress = 0;
                                                         }

                                                         if (markerStartsCluster)
                                                         {
                                                            recsInProgress.Add(rec);
                                                            countInProgress++;
                                                         }

                                                         previousRec = rec;

                                                      }, new ExecutionDataflowBlockOptions
                                                      {
                                                         BoundedCapacity = _intakeBufferSize,
                                                         SingleProducerConstrained = true,
                                                         CancellationToken = userCTkn
                                                      }
                                                    );

         //Propagate RanToCompletion from inBlock to outBlock
         inBlock.Completion.ContinueWith(async t =>
         {  // RanToCompletion, but last cluster and/or foot cluster may still need to be posted
            //We need to be careful here... the RanToCompletion status can only be propagated if the last cluster was posted successfully.
            //Otherwise, there are 2 possibilities:
            //           • awaiting SendAsync (to outBlock) was unblocked (canceled by errorCTkn) - in this case, outBlock needs to be faulted 
            //           • outBlock declined the message (b/c it was canceled by userCTkn) - in this case we do nothing (outBlock is already canceled)
            switch (await PostFinalClustersAsync(countInProgress, recsInProgress, outBlock, errorCTkn, lastRecNo))
            {
               case SendAsyncOutcome.Success:
                  outBlock.Complete();
                  break;
               case SendAsyncOutcome.Unblocked:  //last cluster not posted due to error somewhere in pipeline
                  ((IDataflowBlock)outBlock).Fault(_errorThatCausedShutdown);
                  break;
               case SendAsyncOutcome.Declined:   //last cluster not posted as outBlock was canceled
                  break;
            }
         }, TaskContinuationOptions.OnlyOnRanToCompletion);

         //Propagate Faulted from inBlock to outBlock
         inBlock.Completion.ContinueWith(t => ((IDataflowBlock)outBlock).Fault(t.Exception), TaskContinuationOptions.OnlyOnFaulted);

         //No need to propagate Canceled as both inBlock and outBlock use the same cancellation token (so they are both canceled)
         //TODO: Consider explicit propagation of cancellation between inBlock and outBlock, so that the entire encapsulated block is canceled as a
         //      whole (and not each block separately); this is to improve consistency of inClstrCnt (incremented in PostClusterAsync) with inRecCnt.

         return DataflowBlock.Encapsulate(inBlock, outBlock);
      }


      /// <summary>
      /// Wrapper over the PostClusterAsync function to send the final clusters, i.e. the last regular cluster and foot cluster (either or both optional)
      /// </summary>
      /// <param name="countInProgress"></param>
      /// <param name="recsInProgress"></param>
      /// <param name="outBlock"></param>
      /// <param name="errorCTkn"></param>
      /// <param name="lastRecNoSoFar"></param>
      /// <returns></returns>
      private async Task<SendAsyncOutcome> PostFinalClustersAsync(int countInProgress, List<KeyValRecord> recsInProgress, BufferBlock<KeyValCluster> outBlock, CancellationToken errorCTkn, int lastRecNoSoFar)
      {
         var lastRecNoForFootClstr = lastRecNoSoFar;
         //Send the pending (last regular_ cluster if there are records in progress left
         // note that there should be some record(s) in progress left, unless markerStartsCluster=false and marker(...)=true on the last record
         var lastClusterOutcome = SendAsyncOutcome.Success;
         if (countInProgress > 0)
         {
            lastClusterOutcome = await PostClusterAsync(countInProgress, recsInProgress, outBlock, errorCTkn, 0);
            lastRecNoForFootClstr = recsInProgress.Last().RecNo;
         }
         if (lastClusterOutcome != SendAsyncOutcome.Success) return lastClusterOutcome;

         //Send foot cluster if applicable
         if (_config.AppendFootCluster) lastClusterOutcome = await PostClusterAsync(0, Enumerable.Empty<KeyValRecord>(), outBlock, errorCTkn, lastRecNoForFootClstr);

         return lastClusterOutcome;
      }


      /// <summary>
      /// Helper function for the clustering block to create cluster from cumulated records and asynchronously post it to output buffer block.
      /// Also, raise Intake ProgressChanged event if needed.
      /// </summary>
      /// <param name="countInProgress">Number of records to be posted, i.e. recsToPost.Count (included for performance).</param>
      /// <param name="recsToPost">Cumulated records to be posted as a cluster.</param>
      /// <param name="outBlock">Block to post the cluster to.</param>
      /// <param name="errorCTkn">Cancellation token to facilitate cancellation due to an error somewhere in the pipeline.</param>
      /// <param name="lastRecNoForFootClstr">In case of foot cluster, it is RecNo of the last record; in all other cases (i.e. either regular cluster or head cluster) it must be zero.</param>
      /// <returns>A task returning outcome of the operation.</returns>
      private async Task<SendAsyncOutcome> PostClusterAsync(int countInProgress, IEnumerable<KeyValRecord> recsToPost, BufferBlock<KeyValCluster> outBlock, CancellationToken errorCTkn, int lastRecNoForFootClstr)
      {
         //Send HeadCluster if applicable
         SendAsyncOutcome headClusterOutcome = SendAsyncOutcome.Success;
         if (_beforeHeadCluster.FirstUse && _config.PrependHeadCluster) headClusterOutcome = await PostClusterAsync(0, Enumerable.Empty<KeyValRecord>(), outBlock, errorCTkn, 0);
         if (headClusterOutcome != SendAsyncOutcome.Success) return headClusterOutcome;

         //Determine starting/ending record numbers for the cluster
         int startRecNo, startSourceNo, endRecNo;
         if (recsToPost.Any())
         {
            var firstRec = recsToPost.First();
            startRecNo = firstRec.RecNo;
            startSourceNo = firstRec.SourceNo;
            endRecNo = startRecNo + countInProgress - 1;
         }
         else  //empty cluster means either HeadCluster or FootCluster
         {
            startRecNo = lastRecNoForFootClstr == 0 ? Constants.HeadClusterRecNo : Constants.FootClusterRecNo; // special StartRecNo values of 0 (head cluster) or -1 (foot cluster)
            startSourceNo = 1; // default value
            endRecNo = lastRecNoForFootClstr;  // in case of head cluster, it will be 0, which is what we need
         }

         //Update record and cluster counts for Intake phase in a thread-safe manner:
         var updatedStatus = SafelyUpdateStatus(ref _inStatus, s => s.UpdateRunningCounts(endRecNo, s.ClusterCount + 1));  //recNo is same as recCnt in case of Intake
         int inClstrCnt = updatedStatus.ClusterCount;

         //Note the use of outBlock.SendAsync (and not outBlock.Post), which awaits in case outBlock is full, i.e. throttles based on BoundedCapacity/IntakeBufferSize
         // (outBlock.Post would've simply rejected cluster and returned false in case outBlock is full)
         var sendTask = outBlock.SendAsync(new KeyValCluster(recsToPost, inClstrCnt, startRecNo, startSourceNo, _globalCache, null, _typeDefinitions, _config, ProcessingStatusSupplier), errorCTkn);
         bool sendOK = false; //true = accepted, false = declined
         try
         {
            sendOK = await sendTask;
         }
         catch (OperationCanceledException) { return SendAsyncOutcome.Unblocked; }  //error somewhere in pipeline
         // As a side note, in case of a blocking call (i.e. .Result or .Wait as opposed to the above await), the original exception gets
         //  wrapped in an AggregateException, so the catch clause above would need to be changed to:
         //      catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)

         if (!sendOK) return SendAsyncOutcome.Declined;  //user requested cancellation

         // Note that early performance tests (v0.63 around 3/2/2016) suggested 20+% time reduction under heavy throttling (low IntakeBufferSize)
         // when instead of awaiting sendTask a blocking call that synchronously waits for task result was used, like this:
         //      outBlock.SendAsync.Wait();

         if (ProgressChangedNeedsToBeRaised(inClstrCnt))
         {
            //Intake ProgressChanged event:
            InvokeProgressChanged(new ProgressEventArgs(Phase.Intake, inClstrCnt, inClstrCnt, endRecNo, startRecNo, NotApplicable, _globalCache));  //recCnt points to the last record in cluster, recNo points to the first one
         }
         return SendAsyncOutcome.Success;
      }


      /// <summary>
      /// Create the transforming block, a TransformManyBlock that transforms input clusters to output clusters
      /// </summary>
      /// <param name="transformProvider">Supplies Transform function, a function that takes a single cluster and returns a sequence of resulting clusters</param>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <returns></returns>
      private TransformManyBlock<KeyValCluster, KeyValCluster> CreateTransformingBlock(TransformProvider transformProvider, CancellationToken userCTkn)
      {
         var tranBlock = new TransformManyBlock<KeyValCluster, KeyValCluster>(async cluster =>
         {
            if (_transformationHasStarted.FirstUse)
            {  //transformation not yet started
               SafelyUpdateStatus(ref _tranStatus[FirstTransfmr], s => s.UpdateRunningCounts(s.RecordCount, s.ClusterCount));  //change PhaseState from NotYetStarted to Running
               //                  Note that due to concurrency in Transformation Phase, it is possible for another UpdateRunningCounts to have updated the counts (by the time 
               //                  the UpdateRunningCounts gets a chance to execute; hence s...Count need to be used above (and not 0 i.e. AtStart)
               //Transformation PhaseStarting event:
               _config.Logger.LogInfo("Transformation phase starting...");
               if (_config.ReportProgress) InvokePhaseStarting(new PhaseEventArgs(Phase.Transformation, AtStart, AtStart, FirstTransfmr, _globalCache));
            }
            int clstrNo = cluster.ClstrNo;

            //Synchronize cluster sequence: head cluster (if any) .. "regular" clusters .. foot cluster (if any)
            if (cluster.StartRecNo > 0)  //"regular" cluster - assure it follows head cluster if present
            {
               if (_config.PrependHeadCluster)
               {  //wait until the first cluster (head cluster) has been processed
                  while (_tranStatus[FirstTransfmr].ClusterCount == 0) await Task.Delay(_config.ClusterSyncInterval);
               }
            }
            else if (cluster.StartRecNo == Constants.FootClusterRecNo)  //assure foot cluster is processed last
            {  //wait until all clusters but one (the foot cluster itself) have been processed
               while (_tranStatus[FirstTransfmr].ClusterCount < _inStatus.ClusterCount - 1) await Task.Delay(_config.ClusterSyncInterval);
            }

            IEnumerable<KeyValCluster> resultingClusters = null;
            try
            {
               resultingClusters = transformProvider.TransformAndSetFields(cluster);
            }
            catch (Exception ex)
            {
               //By all likelihood, this exception was thrown by a caller-supplied transformer function (or type definer function).  We need to flag
               // _pipelineErrorCancellationSource right away in order to unblock awaited calls elsewhere, such as SendAsync blocked by full buffer.  We are
               // re-throwing the exception, so that this block is faulted and standard dataflow propagation mechanism can take over. Note that this (re-thrown)
               // exception will be caught in code that awaits block completion, where it can be "swallowed" (as dataflow takes care of automatic fault propagation).
               //See also OnlyOnFaulted continuation below, which faults the beginning of pipeline.
               InitiateShutdown(ex, "transforming block", $" at cluster #{clstrNo.ToString()}");
               throw;
            }

            int recCntInClstr = cluster.Count;

            //Accumulate running cluster & record counts for transformation phase (thread safe):
            var updatedStatus = SafelyUpdateStatus(ref _tranStatus[FirstTransfmr], s => s.UpdateRunningCounts(s.RecordCount + recCntInClstr, s.ClusterCount + 1));

            var tranClstrCnt = updatedStatus.ClusterCount;

            if (ProgressChangedNeedsToBeRaised(tranClstrCnt))
            {
               //Transformation ProgressChanged event:
               InvokeProgressChanged(new ProgressEventArgs(Phase.Transformation, tranClstrCnt, clstrNo, updatedStatus.RecordCount, cluster.StartRecNo, FirstTransfmr, _globalCache));
            }
            return resultingClusters;
         }, new ExecutionDataflowBlockOptions
         {
            BoundedCapacity = _transformInBufferSize,
            SingleProducerConstrained = true,
            MaxDegreeOfParallelism = _config.ConcurrencyLevel,
            CancellationToken = userCTkn
         });

         return tranBlock;
      }


      /// <summary>
      /// Create the holding block, a buffer to hold cluster created during transformation
      /// </summary>
      /// <param name="userCTkn"></param>
      /// <returns></returns>
      private BufferBlock<KeyValCluster> CreateHoldingBlock(CancellationToken userCTkn)
      {
         //Note that the transforming block cannot be directly linked to the unclustering block, as it would cause deadlock in case of RFO#4.
         // (if RFO#4, the link is not established until completion of transformation, which requires the transforming block to be linked).

         return new BufferBlock<KeyValCluster>(new DataflowBlockOptions {
                                                                           BoundedCapacity = _transformOutBufferSize,
                                                                           CancellationToken = userCTkn
                                                                        });
      }


      /// <summary>
      /// Create the unclustering block, a TransformManyBlock that transforms clusters into the KeyVal records contained in them.
      /// </summary>
      /// <param name="router">Determines TargetNo for each record of the cluster.</param>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation.</param>
      /// <returns></returns>
      private TransformManyBlock<KeyValCluster, KeyValRecord> CreateUnclusteringBlock(Router router, CancellationToken userCTkn)
      {
         return new TransformManyBlock<KeyValCluster, KeyValRecord>(clstr =>
         {
            if (_outputHasStarted.FirstUse)
            {  //output not yet started
               SafelyUpdateStatus(ref _outStatus, s => s.UpdateRunningCounts(s.RecordCount, s.ClusterCount));  //change output PhaseState from NotYetStarted to Running (both counts should be AtStart when UpdateRunningCounts executes)
               //Output PhaseStarting event:
               _config.Logger.LogInfo("Output phase starting...");
               if (_config.ReportProgress) InvokePhaseStarting(new PhaseEventArgs(Phase.Output, AtStart, AtStart, NotApplicable, _globalCache));
            }

            int recCntInClstr = clstr.Count;

            //Update record and cluster counts for Output phase in a thread-safe manner:
            var updatedStatus = SafelyUpdateStatus(ref _outStatus, s => s.UpdateRunningCounts(s.RecordCount + recCntInClstr, s.ClusterCount + 1));

            int outClstrCnt = updatedStatus.ClusterCount;   //outClstrCnt and clstr.ClstrNo may not be the same in case clusters were added/removed during Transformation
            if (ProgressChangedNeedsToBeRaised(outClstrCnt))
            {
               //Output ProgressChanged event
               InvokeProgressChanged(new ProgressEventArgs(Phase.Output, outClstrCnt, clstr.ClstrNo, updatedStatus.RecordCount, clstr.StartRecNo, NotApplicable, _globalCache));
            }

            ICluster routedClstr;
            try
            {
               routedClstr = router.Route(clstr);
            }
            catch (Exception ex) //by all likelihood, exception thrown by a caller-supplied function
            {
               InitiateShutdown(ex, "unclustering block", $" at cluster #{clstr.ClstrNo.ToString()}");
               throw;
            }

            return routedClstr.Records.Cast<KeyValRecord>();

         }, new ExecutionDataflowBlockOptions
         {
            BoundedCapacity = _transformOutBufferSize,
            SingleProducerConstrained = true,
            CancellationToken = userCTkn
         });
      }


      /// <summary>
      /// Create the formatting block, a TransformBlock that transforms a KeyVal record into the output line (along with target number)
      /// </summary>
      /// <param name="outputProvider">Supplies either: 2 functions that are specific to the output data type (and match each other): TokenFromItem and TokenJoiner; ; or ExtItemFromItem function.</param>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <returns></returns>
      private TransformBlock<KeyValRecord, Tuple<ExternalLine, int>> CreateFormattingBlock(OutputProvider outputProvider, CancellationToken userCTkn)
      {
         Func<IItem, int, string> tokenFromItem = outputProvider.TokenFromItem;                // A function that takes an item (KeyValItem) along with field# (i.e. it's seq#) and returns a token (string)
         Func<IEnumerable<string>, ExternalLine> tokenJoiner = outputProvider.TokenJoiner;     // A matching function that takes a sequence of string tokens and returns an output line
         Func<IItem, Tuple<string, object>> extItemFromItem = outputProvider.ExtItemFromItem;  // Replaces TokenFromItem/TokenJoiner in case of XrecordOutputProvider

         return new TransformBlock<KeyValRecord, Tuple<ExternalLine, int>>(rec =>
         {
            //Take a KeyVal record (containing a set of KeyVal items), combine it's contents into an output line (string) and return it
            int i = 0;
            ExternalLine line;
            try
            {
               switch (_config.OutputDataKind.ExternalLineType())
               {
                  case ExternalLineType.Xtext: line = tokenJoiner(_outputProvider.ItemsToOutput(rec).Select(itm => tokenFromItem(itm, i++))); break;
                  case ExternalLineType.Xrecord: line = _outputProvider.ItemsToOutput(rec).Select(itm => extItemFromItem(itm)).ToExternalLine(rec.ClstrNo); break;
                  case ExternalLineType.Xsegment: line = tokenJoiner(_outputProvider.ItemsToOutput(rec).Select(itm => tokenFromItem(itm, i++))); ; break;  //TODO: A separate formatter for Xsegment
                  default: throw new NotSupportedException($"No formula to format {_config.InputDataKind} data kind.");
               }
            }
            catch (Exception ex) //no caller-supplied function here, but TokenJoiner can throw DataMisalignedException exception if X12 ISA segment is incorrect
            {
               InitiateShutdown(ex, "formatting block", $" at record #{rec.RecNo.ToString()}");
               throw;
            }
            return line.ToTuple(rec.TargetNo);

         }, new ExecutionDataflowBlockOptions
         {
            BoundedCapacity = _outputBufferSize,
            SingleProducerConstrained = true,
            CancellationToken = userCTkn
         });
      }


      /// <summary>
      /// Create the output block, an ActionBlock that sends output lines (along with target numbers) for output processing
      /// </summary>
      /// <param name="outputProvider"></param>
      /// <param name="userCTkn">Cancellation token to facilitate client (user) requested cancellation</param>
      /// <returns></returns>
      private ActionBlock<Tuple<ExternalLine, int>> CreateOutputBlock(OutputProvider outputProvider, CancellationToken userCTkn)
      {
         var options = new ExecutionDataflowBlockOptions
         {
            BoundedCapacity = _outputBufferSize,
            SingleProducerConstrained = true,
            CancellationToken = userCTkn
         };

         ActionBlock<Tuple<ExternalLine, int>> outBlock = null;

         if (_config.AsyncOutput)
         {
            outBlock = new ActionBlock<Tuple<ExternalLine, int>>(async tpl =>
            {
               try
               {
                  await outputProvider.SendLineToOutputAsync(tpl);
               }
               catch (Exception ex) //could be exception thrown by a caller-supplied output consumer function or other problem, such as:
               {                    // ArgumentOutOfRangeException in case targetNo (tpl.Item2) points to non-existing target or
                                    // ArgumentException ("Invalid name character in ...") if XML output and field name is not a valid XML name
                  var ln = tpl?.Item1;
                  InitiateShutdown(ex, "output block", $" at {(ln?.Type == ExternalLineType.Xrecord ? "record" : "line")} starting with '{ln?.Excerpt}'");
                  throw;
               }
            }, options);
         }
         else  //output is synchronous
         {
            outBlock = new ActionBlock<Tuple<ExternalLine, int>>(tpl =>
            {
               try
               {
                  outputProvider.SendLineToOutput(tpl);
               }
               catch (Exception ex) //could be exception thrown by a caller-supplied output consumer function or other problem, such as:
               {                    // ArgumentOutOfRangeException in case targetNo (tpl.Item2) points to non-existing target or
                                    // ArgumentException ("Invalid name character in ...") if XML output and field name is not a valid XML name
                  var ln = tpl?.Item1;
                  InitiateShutdown(ex, "output block", $" at {(ln?.Type == ExternalLineType.Xrecord ? "record" : "line")} starting with '{ln?.Excerpt}'");
                  throw;
               }
            }, options);
         }

         return outBlock;
      }


      /// <summary>
      /// Starts the UnclusteringBlock, i.e. the Output phase.
      /// Expected to be called when all elements needed for output (such as FieldsToUse) have been set
      /// <param name="fieldsToUse">Set of fields to be used on output; null means "don't bother assigning" (they're either not needed (KW output) or have already been assigned)</param>
      /// </summary>
      private void ReadyForOutput(IReadOnlyList<string> fieldsToUse)
      {
         if (_config.DeferOutput == DeferOutput.Indefinitely) return;

         if (fieldsToUse != null) _outputProvider.SetFieldsToUse(fieldsToUse);

         if (_unclusteringBlockIsLinked.FirstUse)
         {  //UnclusteringBlock not yet linked
            //before linking UnclusteringBlock, we need to send LeaderContents and/or HeaderRow to output (as applicable)

            //Submit header row to output as needed (null means no header row)
            int sNo = 0;
            var hdrLine = _config.OutputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstOutputRow ?
                          _outputProvider.TokenJoiner(_outputProvider.FieldsToUse.Select(fn => _outputProvider.TokenForHeaderRow(fn, sNo++))) :
                          null;
            _outputProvider.AssignLeadersAndHeader(hdrLine?.Text);

            //Link holding and unclustering blocks, so that the output phase can start
            _linkHtoU = _holdingBlock.LinkTo(_unclusteringBlock, new DataflowLinkOptions { PropagateCompletion = true });  //TODO: Consider other block options
         }
      }  //ReadyForOutput


      /// <summary>
      /// Starts the TransformingBlock, i.e. the Transformation phase.
      /// </summary>
      private void ReadyForTransform()
      {
         if (_transformingBlockIsLinked.FirstUse)
         {
            //Link clustering and transforming blocks, so that the transformation phase can start
            _linkCtoT = _clusteringBlock.LinkTo(_transformingBlock, new DataflowLinkOptions { PropagateCompletion = true });  //TODO: Consider other block options
         }
      }


      /// <summary>
      /// Determine if ProgressChanged event (of any phase) needs to be raised for given cluster
      /// </summary>
      /// <param name="clstrSeqNo">Cluster sequence number</param>
      /// <returns></returns>
      private bool ProgressChangedNeedsToBeRaised(int clstrSeqNo)
      {
         if (_config.ProgressInterval == 0) return false;  //interval of 0 means "do not report ProgressChanged event"
         return _config.ReportProgress && clstrSeqNo % _config.ProgressInterval == 0;
      }


      /// <summary>
      /// Safely invoke PhaseStarting event
      /// </summary>
      /// <param name="args">Event arguments</param>
      private void InvokePhaseStarting(PhaseEventArgs args)
      {
         try { PhaseStarting?.Invoke(this, args); }
         catch { /* ignore any possible exception that may be thrown by any handler */}
      }
      /// <summary>
      /// Safely invoke PhaseFinished event
      /// </summary>
      /// <param name="args">Event arguments</param>
      private void InvokePhaseFinished(PhaseEventArgs args)
      {
         try { PhaseFinished?.Invoke(this, args); }
         catch { /* ignore any possible exception that may be thrown by any handler */}
      }
      /// <summary>
      /// Safely invoke ProgressChanged event
      /// </summary>
      /// <param name="args">Event arguments</param>
      private void InvokeProgressChanged(ProgressEventArgs args)
      {
         try { ProgressChanged?.Invoke(this, args); }
         catch { /* ignore any possible exception that may be thrown by any handler */}
      }
      /// <summary>
      /// Safely invoke ErrorOccurred event
      /// </summary>
      /// <param name="args">Event arguments</param>
      private void InvokeErrorOccurred(ErrorEventArgs args)
      {
         try { ErrorOccurred?.Invoke(this, args); }
         catch { /* ignore any possible exception that may be thrown by any handler */}
      }


      /// <summary>
      /// This method is expected to be called when an exception is caught in one of the dataflow blocks (likely thrown by caller supplied code)
      /// In addition to notifications, it flags _pipelineErrorCancellationSource in order to unblock awaited calls elsewhere, such as SendAsync blocked by full buffer.
      /// </summary>
      /// <param name="ex">Exception that was unhandled in one of the blocks (likely thrown by the caller supplied code)</param>
      /// <param name="origin">Dataflow block where the exception occurred</param>
      /// <param name="context">Additional exception context, e.g. cluster number</param>
      private void InitiateShutdown(Exception ex, string origin, string context)
      {
         if (_pipelineErrorOccurred.FirstUse)
         {  //Here, we are processing the very first occurrence of exception 
            // (note that due to multi-threading, the InitiateShutdown function can be called more than once)
            _config.Logger.LogFatal($"Initiating shutdown due to an error in {origin}{context}.", ex);  //due to multi-threading, it's possible to get here more than once
            InvokeErrorOccurred(new ErrorEventArgs(origin, context, ex, _globalCache));
            ex.Source = origin;
            _errorThatCausedShutdown = ex;
            //// Interlocked.CompareExchange(ref _errorThatCausedShutdown, ex, null);  //thread-safe "if (_errorThatCausedShutdown == null) _errorThatCausedShutdown = ex;"
            _pipelineErrorCancelSource.Cancel();
           _userOrTimeoutCancelSource.Cancel();
         }
      }


      /// <summary>
      /// Safely update phase processing status.
      /// </summary>
      /// <param name="statusToUpdate">Reference to the phase processing status to be updated.</param>
      /// <param name="updateFormula">Function that calculates new status from old status.</param>
      /// <returns>The resulting (just updated) status.</returns>
      private PhaseStatus SafelyUpdateStatus(ref PhaseStatus statusToUpdate, Func<PhaseStatus, PhaseStatus> updateFormula)
      {
         PhaseStatus oldStatus, newStatus;
         do
         {
            oldStatus = statusToUpdate;
            newStatus = updateFormula(oldStatus);
         }
         while (Interlocked.CompareExchange(ref statusToUpdate, newStatus, oldStatus) != oldStatus);
         return newStatus;
      }


      /// <summary>
      /// Determine current processing status for a given phase.
      /// </summary>
      /// <param name="phase">Intake, Transformation or Output. </param>
      /// <param name="transformNo">Transformer number (0-based); irrelevant in case of Intake or Ouput.</param>
      /// <returns></returns>
      private PhaseStatus ProcessingStatusSupplier(Phase phase, int transformNo)
      {
         switch (phase)
         {
            case Phase.Intake:
               return _inStatus;
            case Phase.Transformation:
               return _tranStatus[transformNo];
            default: // Phase.Output:
               return _outStatus;
         }
      }

#endregion Private methods

   }
}