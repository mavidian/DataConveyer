//EventRecorder.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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
//  - enum EvtType
//  - class EvtInstance
//  - class EventRecorder

using Mavidian.DataConveyer.Common;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace DataConveyer.Tests.TestHelpers
{
   /// <summary>
   /// Type of DataConveyer progress event: Starting, Progress or Finished.
   /// </summary>
   internal enum EvtType { Starting, Progress, Finished }

   /// <summary>
   /// Individual instance of an event to be reported
   /// </summary>
   internal class EvtInstance
   {
      internal readonly int SeqNo;       // overall sequence number (unique for all recorded events)
      internal readonly Phase Phase;     // Intake Transformation Output
      internal readonly EvtType EvtType; // Start Progress Finish
      internal readonly int PrgSeqNo;    // progress seq# (unique within phase) 0 if Start or Finish
      internal readonly int RecCnt;
      internal readonly int ClstrCnt;
      internal readonly int RecNo;       // 0 if Start or Finish
      internal readonly int ClstrNo;     // 0 if Start or Finish
      internal readonly int ThreadId;
      internal EvtInstance(int seqNo, Phase phase, EvtType evtType, int prgSeqNo, int recCnt, int clstrCnt, int recNo, int clstrNo)
      { SeqNo = seqNo; Phase = phase; EvtType = evtType; PrgSeqNo = prgSeqNo; RecCnt = recCnt; ClstrCnt = clstrCnt; RecNo = recNo; ClstrNo = clstrNo; ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId; }
   }

   /// <summary>
   /// Alows tracing (recording) of Data Conveyer events in a thread-safe manner.
   /// </summary>
   internal class EventRecorder
   {
      private readonly object _locker;

      //Event counters
      private int _allEvtCntr;   // counter of all events
      private int _prgsEvtCntrI; // counter of progress events for Intake phase
      private int _prgsEvtCntrT; // counter of progress events for Transform phase
      private int _prgsEvtCntrO; // counter of progress events for Output phase


      //Holder of event instances, i.e. data to assert on
      private readonly ConcurrentBag<EvtInstance> _events;

      internal EventRecorder()
      {
         _events = new ConcurrentBag<EvtInstance>();
         _allEvtCntr = _prgsEvtCntrI = _prgsEvtCntrT = _prgsEvtCntrO = 0;
         _locker = new object();
      }

      /// <summary>
      /// Record an event for a given set of data provided.
      /// </summary>
      /// <param name="phase"></param>
      /// <param name="evtType"></param>
      /// <param name="recCnt"></param>
      /// <param name="clstrCnt"></param>
      /// <param name="recNo"></param>
      /// <param name="clstrNo"></param>
      internal void RecordEvent(Phase phase, EvtType evtType, int recCnt, int clstrCnt, int recNo, int clstrNo)
      {
         int seqNo;
         var prgSeqNo = 0;
         lock (_locker)
         {
            seqNo = ++_allEvtCntr;
            if (evtType == EvtType.Progress)
            {
               if (phase == Phase.Intake) prgSeqNo = ++_prgsEvtCntrI;
               if (phase == Phase.Transformation) prgSeqNo = ++_prgsEvtCntrT;
               if (phase == Phase.Output) prgSeqNo = ++_prgsEvtCntrO;
            }
            _events.Add(new EvtInstance(seqNo, phase, evtType, prgSeqNo, recCnt, clstrCnt, recNo, clstrNo));
         }
      }

      /// <summary>
      /// Return a list of all recorded events sorted in order they occurred
      /// </summary>
      /// <returns></returns>
      internal List<EvtInstance> GetEventList()
      {
         return _events.OrderBy(e => e.SeqNo).ToList();
      }
 
   }

}
