//EtlOrchestrator_tests_ProgressEvents.cs
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


using DataConveyer.Tests.TestHelpers;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_ProgressEvents
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()     // 20 records, 11 clusters
      {
         yield return "@pRECTYPE=XYZ,@pNAME=\"Mary, Ann\",@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Joan,@pNUM=723";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1000";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Betsy,@pNUM=1100";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Alice,@pNUM=1200";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1300";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1400";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1500";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1600";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1700";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1800";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=1900";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=2000";
         yield return "EOF";
      }

      private readonly EventRecorder _eventTracer;

      public EtlOrchestrator_tests_ProgressEvents()
      {
         _config = new OrchestratorConfig
         {
            ReportProgress = true,
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => { };   // throwaway consumer; resulting output is of no relevance to the tests in this class
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";

         _eventTracer = new EventRecorder();
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_SimpleSettings_StartingAndFinishedEventsFired(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //TODO: Consider redoing this test using EvtInstance, _events and SaveEvtInstance

         //arrange

         //The most likely sequence of events is: SI ST SO FI FT FO
         //, which will result in invocation sequence of 123456  (SI=1 ST=2 SO=3 FI=4 FT=5 FO=6)
         int invokeSeq = 0;  // always digits 1-6 denoting event order, e.g. the most likely 123456
         int clstrTotal = 0;
         var locker = new object();
         _config.PhaseStartingHandler = (s, e) =>
         {
            switch (e.Phase)
            {
               case Phase.Intake:  //always 1st
                  lock (locker) {invokeSeq += 1; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Transformation:  //likely 2nd (always after 1)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 2; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Output:  //likely 3rd (always after 2)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 3; clstrTotal += e.ClstrCnt; }
                  break;
            }
         };
         _config.PhaseFinishedHandler = (s, e) =>
         {
            switch (e.Phase)
            {
               case Phase.Intake:  //likely 4th (always after 1)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 4; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Transformation:  //likely 5th (always after 2 and 4)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 5; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Output:  //always 6th (after 3 and 5)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 6; clstrTotal += e.ClstrCnt; }
                  break;
            }
         };
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var invokeList = invokeSeq.ToString().ToList();  // List<char> to easily compare sequence of digits
         invokeList.Count.Should().Be(6);
         invokeList.IndexOf('2').Should().BeGreaterThan(invokeList.IndexOf('1')); // ST after SI, i.e. 2 after 1
         invokeList.IndexOf('3').Should().BeGreaterThan(invokeList.IndexOf('2')); // SO after ST
         invokeList.IndexOf('4').Should().BeGreaterThan(invokeList.IndexOf('1')); // FI after SI
         invokeList.IndexOf('5').Should().BeGreaterThan(invokeList.IndexOf('2')); // FT after ST
         invokeList.IndexOf('5').Should().BeGreaterThan(invokeList.IndexOf('4')); // FT after FI
         invokeList.IndexOf('6').Should().BeGreaterThan(invokeList.IndexOf('3')); // FO after SO
         invokeList.IndexOf('6').Should().BeGreaterThan(invokeList.IndexOf('5')); // FO after FT

         clstrTotal.Should().Be(33); // 3*0 for Starting + 3*11 for Finishing events
      }



      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_AllowToAlterFields_StartingOutputDeferred(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //TODO: Consider redoing this test using EvtInstance, _events and SaveEvtInstance

         //arrange
         _config.AllowTransformToAlterFields = true;  //this setting forces PhaseStarting event for Output wait until Transformation is finished (RFO#4)
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.HeadersInFirstOutputRow = true;
         _config.OutputFields = null;  //needed to reset setting from test ctor; otherwise, the predefined output fields would allow output to start early (RFO#1 instead of RFO#4 needed for this test)

         //Events: (SI=1 ST=2 SO=3 FI=4 FT=5 FO=6)
         //For this test SO comes after FT, e.g. SI ST FI FT SO FO, i.e. 124536
         int invokeSeq = 0;  // always digits 1-6 denoting event order, e.g. 124536
         int clstrTotal = 0;
         var locker = new object();
         _config.PhaseStartingHandler = (s, e) =>
         {
            switch (e.Phase)
            {
               case Phase.Intake:  //always 1st
                  lock (locker) { invokeSeq += 1; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Transformation:  //likely 2nd (always after 1)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 2; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Output:  //likely 3rd (always after 2)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 3; clstrTotal += e.ClstrCnt; }
                  break;
            }
         };
         _config.PhaseFinishedHandler = (s, e) =>
         {
            switch (e.Phase)
            {
               case Phase.Intake:  //likely 4th (always after 1)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 4; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Transformation:  //likely 5th (always after 2 and 4)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 5; clstrTotal += e.ClstrCnt; }
                  break;
               case Phase.Output:  //always 6th (after 3 and 5)
                  lock (locker) { invokeSeq = invokeSeq * 10 + 6; clstrTotal += e.ClstrCnt; }
                  break;
            }
         };
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(21);

         var invokeList = invokeSeq.ToString().ToList();  // List<char> to easily compare sequence of digits
         invokeList.Count.Should().Be(6);
         invokeList.IndexOf('2').Should().BeGreaterThan(invokeList.IndexOf('1')); // ST after SI, i.e. 2 after 1
         invokeList.IndexOf('3').Should().BeGreaterThan(invokeList.IndexOf('5')); // SO after FT
         invokeList.IndexOf('4').Should().BeGreaterThan(invokeList.IndexOf('1')); // FI after SI
         invokeList.IndexOf('5').Should().BeGreaterThan(invokeList.IndexOf('2')); // FT after ST
         invokeList.IndexOf('5').Should().BeGreaterThan(invokeList.IndexOf('4')); // FT after FI
         invokeList.IndexOf('6').Should().BeGreaterThan(invokeList.IndexOf('3')); // FO after SO

         clstrTotal.Should().Be(33); // 3*0 for Starting + 3*11 for Finishing events
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_SetProgressChangedEvents_AllEventsFiredInSequence(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 1;  //enable ProgressChangedHandler (every cluster)
         _config.ConcurrencyLevel = 1;  //no parallelization, so that transformation progress events will fire in sequence

         //This test verifies that for each phase there are 13 events: S, 11Ps and F (in that sequence incl. transformation); however, different phases may inverleave.
         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();
          
         events.Should().HaveCount(39);  // (1 starting + 11 progress change + 1 finished) times 3 phases
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(39);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[38].Phase.Should().Be(Phase.Output);
         events[38].EvtType.Should().Be(EvtType.Finished);
         events[38].PrgSeqNo.Should().Be(0);
         events[38].RecCnt.Should().Be(20);
         events[38].ClstrCnt.Should().Be(11);
         events[38].RecNo.Should().Be(0);
         events[38].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(33);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt > 0 && e.ClstrCnt > 0 && e.RecNo > 0 && e.ClstrNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt <= 20 && e.ClstrCnt <= 11 && e.RecNo <= 20 && e.ClstrNo <= 11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(13);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(13);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(13);
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SetProgressChangedEventsParallel_AllEventsFired(int iterationNumber, int totalRepeats)
      {
         //Note: This test is not exact and may fail because the event order cannot be forced based on time delays.
         //      When repeated 1,000 times, it usually fails 2 times.

         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 1;  //enable ProgressChangedHandler (every cluster)
         _config.ConcurrencyLevel = 4;  //due to parallelization, so that transformation progress events may not come in order

         //This test verifies that for each phase there are 13 events: S, 11Ps and F however, different phases may inverleave.
         //Unlike the prior test (ProcessPipeline_SetProgressChangedEvents_AllEventsFiredInSequence), we cannot guarantee the sequence of the 11 progress events for Transformation (due to parallelization).
         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (in case of parallelization) - but still there is no guarantee
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(39);  // (1 starting + 11 progress change + 1 finished) times 3 phases
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(39);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[38].Phase.Should().Be(Phase.Output);
         events[38].EvtType.Should().Be(EvtType.Finished);
         events[38].PrgSeqNo.Should().Be(0);
         events[38].RecCnt.Should().Be(20);
         events[38].ClstrCnt.Should().Be(11);
         events[38].RecNo.Should().Be(0);
         events[38].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(33);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt > 0 && e.ClstrCnt > 0 && e.RecNo > 0 && e.ClstrNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt <= 20 && e.ClstrCnt <= 11 && e.RecNo <= 20 && e.ClstrNo <= 11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         //The assert below may fail as the no guarantee that the transformation will process the records out of order (even with the delays)
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().NotBeInAscendingOrder(); //notice .Not after Should()
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(13);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(13);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(13);
      }


      [Theory]
      [Repeat(5)]
      public void ProcessPipeline_ProgressEvery3rd_CorrectEventsFired(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 3;
         _config.ConcurrencyLevel = 1; //run transformation sequentially to ensure transformation progress is reported in sequence

         //This test verifies that for each phase there are 5 events: S, 3Ps and F (in that sequence incl. transformation); however, different phases may inverleave.
         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(15);  // (  //1 starting + 3 progress change + 1 finished) times 3 phases
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(15);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[14].Phase.Should().Be(Phase.Output);
         events[14].EvtType.Should().Be(EvtType.Finished);
         events[14].PrgSeqNo.Should().Be(0);
         events[14].RecCnt.Should().Be(20);
         events[14].ClstrCnt.Should().Be(11);
         events[14].RecNo.Should().Be(0);
         events[14].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(9);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.ClstrCnt % 3 == 0);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(5);
      }


      [Theory]
      [Repeat(5)]
      public void ProcessPipeline_ClstrsDoubledProgressEvery3rd_CorrectEventsFired(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 3;
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.Universal;
         _config.UniversalTransformer = c => (new KeyValCluster[] { (KeyValCluster)c, (KeyValCluster)c.GetClone() }).ToList();  // duplicate each cluster during transformation
         _config.ConcurrencyLevel = 1;  //don't parallelize to ensure transformation events occur in order

         //This test verifies that for each phase there are 5 events: S, 3Ps and F (in that sequence incl. transformation); however, different phases may inverleave.
         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(22); //each input cluster got doubled on output
         counts.RowsWritten.Should().Be(40);     //, which doubled the output record count

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(19);  // (1 starting + 3 progress change + 1 finished) * 2 (Intake & Transformation) + (1 starting + 7 progress + 1 finished) for Output
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(19);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[18].Phase.Should().Be(Phase.Output);
         events[18].EvtType.Should().Be(EvtType.Finished);
         events[18].PrgSeqNo.Should().Be(0);
         events[18].RecCnt.Should().Be(40);   //doubled
         events[18].ClstrCnt.Should().Be(22); //doubled
         events[18].RecNo.Should().Be(0);
         events[18].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(13);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.ClstrCnt % 3 == 0);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(7);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Where(e => e.Phase != Phase.Output).Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);
         finEvents.Where(e => e.Phase == Phase.Output).Should().OnlyContain(e => e.RecCnt == 40 && e.ClstrCnt == 22 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(9);
      }


      [Theory]
      [Repeat(5)]
      public void ProcessPipeline_IntervalOf0_NoProgressChangedEvents(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 0;

         //This test is very similar to ProcessPipeline_SimpleSettings_StartingAndFinishedEventsFired
         //It verifies that for each phase there are just 2 events: S and F (no Progress); however, different phases may inverleave.
         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         //the above handler will not be called in this test
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(6);  // (1 starting + 1 finished) * 3 phases
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(6);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[5].Phase.Should().Be(Phase.Output);
         events[5].EvtType.Should().Be(EvtType.Finished);
         events[5].PrgSeqNo.Should().Be(0);
         events[5].RecCnt.Should().Be(20);
         events[5].ClstrCnt.Should().Be(11);
         events[5].RecNo.Should().Be(0);
         events[5].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().BeEmpty();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(2);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(2);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(2);
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SlowTransformBufferUnbounded_AllIntakeFiredFirst(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //Note: This test is not exact and may fail because the event order cannot be forced based on time delays.
         //      When repeated 1,000 times, it usually fails 10-20 times.

         //arrange
         _config.ProgressInterval = 1;
         _config.ClusterFilterPredicate = c => { Thread.Sleep(1); return true; };  // no transformations, data passed as is; however, delay tranformation processing
         _config.ConcurrencyLevel = 1;  //don't parallelize to make the transformation phase long

         //This test verifies progress events only
         _config.ProgressChangedHandler = (s, e) => { _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(33);  // 11 progress change times 3 phases
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(33);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Progress);
         events[0].PrgSeqNo.Should().Be(1);
         events[0].RecCnt.Should().Be(4);
         events[0].ClstrCnt.Should().Be(1);
         events[0].RecNo.Should().Be(1);
         events[0].ClstrNo.Should().Be(1);

         //last
         events[32].Phase.Should().Be(Phase.Output);
         events[32].EvtType.Should().Be(EvtType.Progress);
         events[32].PrgSeqNo.Should().Be(11);
         events[32].RecCnt.Should().Be(20);
         events[32].ClstrCnt.Should().Be(11);
         events[32].RecNo.Should().Be(19);
         events[32].ClstrNo.Should().Be(11);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().BeEmpty();

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(33);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt > 0 && e.ClstrCnt > 0 && e.RecNo > 0 && e.ClstrNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt <= 20 && e.ClstrCnt <= 11 && e.RecNo <= 20 && e.ClstrNo <= 11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         //This assert can fail as there is no guarantee how many intake clusters get processed before first transformation no matter what the delays are.
         //Unfortunately, the 1st transformation event can even occur as #2 (after the first Intake even), so changing Should().Be(12) below to something
         //like Should().BeGreaterThan(7) doesn't help much in eliminating failures.
         prgEvents.First(e => e.Phase == Phase.Transformation).SeqNo.Should().Be(12);  // 1st Transformation event after 11 Intake events

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().BeEmpty();

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(11);
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SlowTransformBufferSizeAt1_IntakeAndTransformAlternate(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //Note: This test is not exact and may fail because the event order cannot be forced based on time delays.
         //      When repeated 1,000 times, it usually fails 2-3 times.

         //arrange
         _config.ProgressInterval = 1;
         _config.BufferSize = 1;
         _config.IntakeBufferFactor = 1.0;
         _config.ClusterFilterPredicate = c => { Thread.Sleep(2); return true; };  // no transformations, data passed as is; however, delay tranformation processing
         _config.ConcurrencyLevel = 1;  //don't parallelize to make the transformation phase long

         //This test verifies progress events only
         //The expected sequence of events is:
         // I1 I2 Tr1 I3 Tr2 I4 Tr3 I5 .. Tr9 I11 Tr10 Tr11  (1st cluster is picked by Tr, so 2nd can complete quickly, but the 3rd and rest 
         // need to wait for slow Tr to complete and pick next cluster and make room in buffer).  Note the Output phase is not reported here.
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase != Phase.Output) _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(22);  // 11 progress change times 2 (Input and Transformation)
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(22);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Progress);
         events[0].PrgSeqNo.Should().Be(1);
         events[0].RecCnt.Should().Be(4);
         events[0].ClstrCnt.Should().Be(1);
         events[0].RecNo.Should().Be(1);
         events[0].ClstrNo.Should().Be(1);

         //last
         events[21].Phase.Should().Be(Phase.Transformation);
         events[21].EvtType.Should().Be(EvtType.Progress);
         events[21].PrgSeqNo.Should().Be(11);
         events[21].RecCnt.Should().Be(20);
         events[21].ClstrCnt.Should().Be(11);
         events[21].RecNo.Should().Be(19);
         events[21].ClstrNo.Should().Be(11);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().BeEmpty();

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(22);
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt > 0 && e.ClstrCnt > 0 && e.RecNo > 0 && e.ClstrNo > 0);
         prgEvents.Should().OnlyContain(e => e.RecCnt <= 20 && e.ClstrCnt <= 11 && e.RecNo <= 20 && e.ClstrNo <= 11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().BeEmpty();

         //The assert below verifies this sequence of events:
         //  1  2  3   4  5   6  7   8 ..  18  19  20  21   22  (SeqNos 2-21 are verifield below)
         // I1 I2 Tr1 I3 Tr2 I4 Tr3 I5 .. I10 Tr9 I11 Tr10 Tr11 
         //This assert can fail as there is no guarantee that Intake will always process cluster faster than Transform even with the delay.
         var expctdInPrgEvts = prgEvents.Where(e => e.SeqNo % 2 == 0).Take(9).ToList();  // SeqNos: 2, 4, 6, .., 18 (note the first and last clstr don't fit the pattern)
         var expctdTrPrgEvts = prgEvents.Skip(4).Where(e => e.SeqNo % 2 == 1).Take(9).ToList(); // evt.SeqNos: 5, 7, 9 .., 21
         expctdInPrgEvts.Zip(expctdTrPrgEvts, (i, t) => i.Phase == Phase.Intake && t.Phase == Phase.Transformation && i.ClstrNo == t.ClstrNo).Should().OnlyContain(x => x);

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().BeEmpty();

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(11);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(11);
         events.Where(e => e.Phase == Phase.Output).Should().BeEmpty();
      }


      [Theory]
      [Repeat(5)]
      public void ProcessPipeline_SomeClstrsRemovedProgressEvery3rd_CorrectEventsFired(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 3;
         _config.AllowOnTheFlyInputFields = true;
         _config.ClusterFilterPredicate = c => c.Count > 1;  // remove those clusters that have only a single record
         _config.ConcurrencyLevel = 1;  //don't parallelize to ensure transformation events occur in order

         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(4);   //there were 7 single record clusters on intake,
         counts.RowsWritten.Should().Be(13);      // so the remaining 4 clusters totaled 13 records

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(13);  // (1 starting + 3 progress change + 1 finished) * 2 (Intake & Transformation) + (1 starting + 1 progress + 1 finished) for Output
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(13);

         //note the coincidence: 13 is both # of rows on output as well as total number of events

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[12].Phase.Should().Be(Phase.Output);
         events[12].EvtType.Should().Be(EvtType.Finished);
         events[12].PrgSeqNo.Should().Be(0);
         events[12].RecCnt.Should().Be(13);  //records within clusters containing 2+ records
         events[12].ClstrCnt.Should().Be(4); //clusters containing 2+ records
         events[12].RecNo.Should().Be(0);
         events[12].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(7);  // 3 + 3 + 1
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.ClstrCnt % 3 == 0);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().ContainSingle();  //i.e. count=1
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Where(e => e.Phase != Phase.Output).Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);
         finEvents.Where(e => e.Phase == Phase.Output).Should().OnlyContain(e => e.RecCnt == 13 && e.ClstrCnt == 4 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(3);
      }


      [Theory]
      [Repeat(5)]
      public void ProcessPipeline_SomeRecsRemovedProgressEvery3rd_CorrectEventsFired(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 3;
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.RecordFilter;
         _config.RecordFilterPredicate = r => ((string)r["NAME"])?.StartsWith("J") != true && (int?)r["NUM"] != 1800;  //recs 7-8 (clstr 2) & rec 17 (entire cluster 9) removed
         _config.ConcurrencyLevel = 1;  //don't parallelize to ensure transformation events occur in order

         _config.PhaseStartingHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventTracer.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventTracer.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(10);   //cluster 17 became empty after record removal, hence it was removed
         counts.RowsWritten.Should().Be(17);       //total of 3 records were removed

         var events = _eventTracer.GetEventList();

         events.Should().HaveCount(15);  // (1 starting + 3 progress change + 1 finished) * 2 (Intake & Transformation) + (1 starting + 3 progress + 1 finished) for Output
         events.Min(e => e.SeqNo).Should().Be(1);
         events.Max(e => e.SeqNo).Should().Be(15);

         //first
         events[0].Phase.Should().Be(Phase.Intake);
         events[0].EvtType.Should().Be(EvtType.Starting);
         events[0].PrgSeqNo.Should().Be(0);
         events[0].RecCnt.Should().Be(0);
         events[0].ClstrCnt.Should().Be(0);
         events[0].RecNo.Should().Be(0);
         events[0].ClstrNo.Should().Be(0);

         //last
         events[14].Phase.Should().Be(Phase.Output);
         events[14].EvtType.Should().Be(EvtType.Finished);
         events[14].PrgSeqNo.Should().Be(0);
         events[14].RecCnt.Should().Be(17);
         events[14].ClstrCnt.Should().Be(10);
         events[14].RecNo.Should().Be(0);
         events[14].ClstrNo.Should().Be(0);

         var strtEvents = events.Where(e => e.EvtType == EvtType.Starting);
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = events.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().HaveCount(9);  // 3 + 3 + 3
         prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
         prgEvents.Should().OnlyContain(e => e.ClstrCnt % 3 == 0);
         prgEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Intake).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Transformation).Select(e => e.ClstrNo).Should().BeInAscendingOrder();
         prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(3);
         prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

         var finEvents = events.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Where(e => e.Phase != Phase.Output).Should().OnlyContain(e => e.RecCnt == 20 && e.ClstrCnt == 11 && e.RecNo == 0 && e.ClstrNo == 0);
         finEvents.Where(e => e.Phase == Phase.Output).Should().OnlyContain(e => e.RecCnt == 17 && e.ClstrCnt == 10 && e.RecNo == 0 && e.ClstrNo == 0);

         events.Where(e => e.Phase == Phase.Intake).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(5);
         events.Where(e => e.Phase == Phase.Output).Should().HaveCount(5);
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SlowIntake_TransformationStartsRightAway(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //Note: This test is not exact and may fail because the event order cannot be forced based on time delays.
         //      When repeated 1,000 times, it failed once or not at all.

         //arrange
         int lastIntakeRecNo = 0;  //running value of intake record# set at Intake
         int lastIntakeRecNoAtTranStart = 0;  //last intake record# when Transformation starts

         _config.RecordInitiator = (r, tb) => { Thread.Sleep(2); lastIntakeRecNo = r.RecNo; return false; };
         _config.PhaseStartingHandler = (s, e) => { if (e.Phase == Phase.Transformation) lastIntakeRecNoAtTranStart = lastIntakeRecNo; };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         //This assert may fail as even slow intake may get additional records processed before transformer picks up
         lastIntakeRecNoAtTranStart.Should().Be(5);  //Intake needed 5 records to post the 1st cluster, so that Transformation could start
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_SlowIntake_DeferredTransformationWaitsForInitiator(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         int lastIntakeRecNo = 0;  //running value of intake record# set at Intake
         int lastIntakeRecNoAtTranStart = 0;  //last intake record# when Transformation starts

         _config.RecordInitiator = (r, tb) =>
         {
           // Thread.Sleep(2);
            lastIntakeRecNo = r.RecNo;
            return r.RecNo == 7;
         };  //transformation can start after 7 records
         _config.DeferTransformation = DeferTransformation.UntilRecordInitiation;
         _config.PhaseStartingHandler = (s, e) =>
         { if (e.Phase == Phase.Transformation) lastIntakeRecNoAtTranStart = lastIntakeRecNo; };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         //lastIntakeRecNoAtTranStart.Should().Be(7);  //Record #7 (2nd cluster) released Transformation, by that time 1st cluster is likely already waiting (but no guarantee), so that Transformation could start
         lastIntakeRecNoAtTranStart.Should().BeGreaterOrEqualTo(7);  //Record #7 (2nd cluster) released Transformation
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_SlowIntake_DeferredTransformationWaitsUntilIntakeEnds(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         int lastIntakeRecNo = 0;  //running value of intake record# set at Intake
         int lastIntakeRecNoAtTranStart = 0;  //last intake record# when Transformation starts

         _config.RecordInitiator = (r, tb) => { Thread.Sleep(1); lastIntakeRecNo = r.RecNo; return false; };  //slow intake to "encourage" transformer to fail the test
         _config.DeferTransformation = DeferTransformation.UntilRecordInitiation;
         _config.PhaseStartingHandler = (s, e) => { if (e.Phase == Phase.Transformation) lastIntakeRecNoAtTranStart = lastIntakeRecNo; };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(20);
         counts.ClustersRead.Should().Be(11);
         counts.ClustersWritten.Should().Be(11);
         counts.RowsWritten.Should().Be(20);

         lastIntakeRecNoAtTranStart.Should().Be(20);  //Transformation did not start until all records got read from Intake
      }
   }
}
