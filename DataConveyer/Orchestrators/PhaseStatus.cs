//PhaseStatus.cs
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

namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// Immutable type that describes the current state of a given phase of the Data Conveyer process.
   /// Instances are returned by the GetProcessingStatus method (of record and cluster).
   /// </summary>
   public class PhaseStatus
   {
      /// <summary>
      /// One of: <see cref="Phase.Intake">Intake</see>, <see cref="Phase.Transformation">Transformation</see> or <see cref="Phase.Output">Output</see>.
      /// </summary>
      public Phase Phase { get; }
      /// <summary>
      /// Transformer sequence number, i.e. 0-based index (always -1 in case of Intake or Output). 
      /// </summary>
      public int TransfmrNo { get; }
      /// <summary>
      /// One of: <see cref="ExecutionState.NotYetStarted">NotYetStarted</see>, <see cref="ExecutionState.Running">Running</see> or <see cref="ExecutionState.Complete">Complete</see>.
      /// </summary>
      public ExecutionState State { get; }
      /// <summary>
      /// Number of records processed so far by the phase.
      /// Becomes total records processed upon phase completion.
      /// </summary>
      public int RecordCount { get; }
      /// <summary>
      /// Number of clusters processed so far by the phase.
      /// Becomes total clusters processed upon phase completion.
      /// </summary>
      public int ClusterCount { get; }

      private PhaseStatus(Phase phase, int transfmrNo, ExecutionState state, int recordCount, int clusterCount)
      {  //only accessible within the class
         Phase = phase;
         TransfmrNo = transfmrNo;
         State = state;
         RecordCount = recordCount;
         ClusterCount = clusterCount;
      }

      /// <summary>
      /// The only externally accessible ctor creates a not-yet-started instance with zero counts
      /// </summary>
      /// <param name="phase"></param>
      /// <param name="transfmrNo"></param>
      internal PhaseStatus(Phase phase, int transfmrNo = EtlOrchestrator.NotApplicable) : this(phase, transfmrNo, ExecutionState.NotYetStarted, EtlOrchestrator.AtStart, EtlOrchestrator.AtStart) { }

      internal PhaseStatus UpdateRunningCounts(int recordCount, int clusterCount)
      {
         if (State == ExecutionState.Complete) return this;  //unlikely, but possible (e.g. SetComplete may be called before ClusteringBlock shuts down due to cancelation)
         return new PhaseStatus(Phase, TransfmrNo, ExecutionState.Running, recordCount, clusterCount);
      }

      internal PhaseStatus SetComplete()
      {
         if (State == ExecutionState.Complete) return this;  //probably not needed
         return new PhaseStatus(Phase, TransfmrNo, ExecutionState.Complete, RecordCount, ClusterCount);
      }
   }
}
