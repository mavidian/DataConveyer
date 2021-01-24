//EventAgruments.cs
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
//  - class ProgressEventArgs
//  - class ErrorEventArgs
//  - class PhaseEventArgs
//
//Classes derived from System.EventArgs; they contain data for events thrown by EtlOrchestrator.

using Mavidian.DataConveyer.Common;
using System;

namespace Mavidian.DataConveyer.Orchestrators
{

   /// <summary>
   /// Arguments for PhaseStarting and PhaseFinished events
   /// </summary>
   public class PhaseEventArgs : EventArgs
   {
      internal PhaseEventArgs(Phase phase, int clstrCnt, int recCnt, int transfmrNo, IGlobalCache globalCache) : base()
      {
         Phase = phase;
         ClstrCnt = clstrCnt;
         RecCnt = recCnt;
         TransfmrNo = transfmrNo;
         GlobalCache = globalCache;
      }

      /// <summary>
      /// Phase of the pipeline: Intake, Transformation or Output
      /// </summary>
      public Phase Phase { get; private set; }
      /// <summary>
      /// Total number of clusters processed by the phase (0 in case of PhaseStarting event).
      /// </summary>
      public int ClstrCnt { get; private set; }
      /// <summary>
      /// Total number of records processed by the phase (0 in case of PhaseStarting event).
      /// </summary>
      public int RecCnt { get; private set; }
      /// <summary>
      /// Zero based transformer's sequence number for Transformation phase (values greater than 0 reserved for future use).
      /// Always -1 in case of Intake or Output phase.
      /// </summary>
      public int TransfmrNo { get; private set; }
      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; private set; }
   }


   /// <summary>
   /// Arguments for ProgressChanged event
   /// </summary>
   public class ProgressEventArgs : EventArgs
   {
      internal ProgressEventArgs(Phase phase, int clstrCnt, int clstrNo, int recCnt, int recNo, int transfmrNo, IGlobalCache globalCache) : base()
      {
         Phase = phase;
         ClstrCnt = clstrCnt;
         ClstrNo = clstrNo;
         RecCnt = recCnt;
         RecNo = recNo;
         TransfmrNo = transfmrNo;
         GlobalCache = globalCache;
      }

      /// <summary>
      /// Phase of the pipeline: Intake, Transformation or Output
      /// </summary>
      public Phase Phase { get; private set; }
      /// <summary>
      /// Number of clusters processed so far by the phase, including current cluster.
      /// In case of Transformation phase, this number refers to clusters before transformation, and not after (note that transformation may add or delete clusters).
      /// </summary>
      public int ClstrCnt { get; private set; }
      /// <summary>
      /// Sequence number of current cluster (1 based).
      /// This number is assigned on Intake and in other phases may be different than ClstrCnt in case of cluster additions/deletions during transformations.
      /// Example: if clusters are doubled in transform, then ClstrCnt value in output are: 1,2,3,4,... while ClstrNo values are: 1,1,2,2,...
      /// </summary>
      public int ClstrNo { get; private set; }
      /// <summary>
      /// Number of records processed so far by the phase, including all records in current cluster.
      /// In case of Transformation phase, this number refers to records before transformation, and not after (note that transformation may add or delete records).
      /// In Intake phase, header row does not count towards RecCnt.
      /// In Output phase, leader, header and trailer rows do not count towards RecCnt.
      /// </summary>
      public int RecCnt { get; private set; }
      /// <summary>
      /// Sequence number (1 based) of the first record in current cluster as it appeared on Intake (i.e. before any transformations).
      /// This number is determined when clusters are formed during Intake and remains unchanged even as records are added or deleted during transformations; it corresponds to the cluster's StartRecNo.
      /// Note 2 special values: 0 (<see cref="Constants.HeadClusterRecNo"/>) in case of head cluster and -1 (<see cref="Constants.FootClusterRecNo"/> in case of foot cluster.
      /// </summary>
      public int RecNo { get; private set; }
      /// <summary>
      /// Transformer's sequence number for Transformation phase; always -1 for Intake and Output
      /// </summary>
      public int TransfmrNo { get; private set; }
      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; private set; }
   }


   /// <summary>
   /// Arguments for ErrorOccurred event
   /// </summary>
   public class ErrorEventArgs : EventArgs
   {
      internal ErrorEventArgs(string origin, string context, Exception exception, IGlobalCache globalCache) : base()
      {
         Origin = origin;
         Context = context;
         Exception = exception;
         GlobalCache = globalCache;
      }

      /// <summary>
      /// Stage of the Data Conveyer process where the error occurred, e.g. "line parsing block"
      /// </summary>
      public string Origin { get; private set; }
      /// <summary>
      /// Additional information about the error context, e.g. " at line #2"
      /// </summary>
      public string Context { get; private set; }
      /// <summary>
      ///  Unhandled exception that caused the error
      /// </summary>
      public Exception Exception { get; private set; }
      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; private set; }
   }

}
