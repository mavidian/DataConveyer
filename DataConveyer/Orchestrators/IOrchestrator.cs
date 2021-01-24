//IOrchestrator.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


//Contents:
//  - interface IOrchestrator
//  - struct ProcessResult
//  - enum CompletionStatus

using System;
using System.Threading.Tasks;


namespace Mavidian.DataConveyer.Orchestrators
{

   /// <summary>
   /// Interface defining members of the pipeline orchestrator.
   /// </summary>
   public interface IOrchestrator : IDisposable
   {
      /// <summary>
      /// Outcome of initializations that occur at start of the ExecuteAsync method, such as opening intake sources and output targets.
      /// False denotes successful completion of initializations.
      /// True means a failure, in which case ExecuteAsync method will always result in CompletionStatus.InitializationError.
      /// </summary>
      bool InitErrorOccurred { get; }

      /// <summary>
      /// Executes the orchestrated pipeline asynchronously.
      /// </summary>
      /// <returns>A task containing processing results.</returns>
      Task<ProcessResult> ExecuteAsync();

      /// <summary>
      /// Aborts execution in progress, if any.
      /// </summary>
      void CancelExecution();
   }


   /// <summary>
   /// Result from a task returned by the orchestrator execution.
   /// </summary>
   public class ProcessResult
   {
      /// <summary>
      /// Reason for completion of process execution.
      /// </summary>
      public CompletionStatus CompletionStatus { get; }

      //TODO: Note that record/cluster concept is specific to EtlOrchestrator, consider making cluster counts more generic (rename to auxCount(?))      
      /// <summary>
      /// Number of all rows read from intake, including header row(s) if applicable.
      /// </summary>
      public int RowsRead { get; }
      /// <summary>
      /// Number of clusters created during intake process. Head cluster and/or foot cluster are included if present.
      /// </summary>
      public int ClustersRead { get; }
      /// <summary>
      /// Number of output clusters created. Head cluster and/or foot cluster are included if present on output. 
      /// </summary>
      public int ClustersWritten { get; }
      /// <summary>
      /// Number of all rows sent to output, including rows constituting leader, header, head cluster, foot cluster and trailer as applicable.
      /// </summary>
      public int RowsWritten { get; }

      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; }


      internal ProcessResult(CompletionStatus completionStatus, int rowsRead, int clustersRead, int clustersWritten, int rowsWritten, IGlobalCache globalCache)
      {
         CompletionStatus = completionStatus;
         RowsRead = rowsRead;
         ClustersRead = clustersRead;
         ClustersWritten = clustersWritten;
         RowsWritten = rowsWritten;
         GlobalCache = globalCache;
      }
   }


   /// <summary>
   /// Possible reasons for completion of process execution.
   /// </summary>
   public enum CompletionStatus
   {
      /// <summary>
      /// The process completed successfully by exhausting intake data (e.g. end of input file reached).
      /// </summary>
      IntakeDepleted,
      /// <summary>
      /// The process terminated upon processing the maximum number of intake records allowed (specified in the <see cref="OrchestratorConfig.IntakeRecordLimit"/> setting).
      /// </summary>
      LimitReached,
      /// <summary>
      /// The process was canceled by an external source (a call to <see cref="IOrchestrator.CancelExecution()"/> method).
      /// </summary>
      Canceled,
      /// <summary>
      /// Execution time exceeded the maximum time allowed (specified in the <see cref="OrchestratorConfig.TimeLimit"/> setting).
      /// </summary>
      TimedOut,
      /// <summary>
      /// The process could not start due to an error during initialization of the core process orchestration. For example, the input file was not found.
      /// Details can be available in log data.
      /// </summary>
      InitializationError,
      /// <summary>
      /// The process could not start as the <see cref="IOrchestrator.ExecuteAsync()"/> method call was attempted in an invalid context,
      /// such as after prior execution or on disposed orchestrator object.
      /// </summary>
      InvalidAttempt,
      /// <summary>
      /// An irrecoverable error occurred during processing. For example, an exception thrown by a caller supplied code.
      /// </summary>
      Failed
   }

}
