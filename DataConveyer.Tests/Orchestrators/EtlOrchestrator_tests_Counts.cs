//EtlOrchestrator_tests_Counts.cs
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

using DataConveyer.Tests.TestHelpers;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   //Alias for the tuple that holds definition of the TraceableAsserter output, i.e. (Ext,Header,Formatter,ExcFormatter):
   using AsserterOutput = ValueTuple<string, string, Func<EvtInstance, string>, Func<Exception, IEnumerable<string>>>;

   public class EtlOrchestrator_tests_Counts
   {

      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()  // 68 lines: 1 header line + 67 data lines
      {
         yield return "FNAME,LNAME,CITY,STATE,ZIP";
         yield return "Carin,Deleo,Little Rock,AR,72202";
         yield return "Mattie,Poquette,Phoenix,AZ,85013";
         yield return "Arminda,Parvis,Phoenix,AZ,85017";
         yield return "Regenia,Kannady,Scottsdale,AZ,85260";
         yield return "Keneth,Borgman,Phoenix,AZ,85012";
         yield return "Dick,Wenzinger,Gardena,CA,90248";
         yield return "Daniel,Perruzza,Santa Ana,CA,92705";
         yield return "Cristal,Samara,Los Angeles,CA,90021";
         yield return "Rasheeda,Sayaphon,Saratoga,CA,95070";
         yield return "Refugia,Jacobos,Hayward,CA,94545";
         yield return "Dorothy,Chesterfield,San Diego,CA,92126";
         yield return "Gail,Similton,Thousand Palms,CA,92276";
         yield return "Thaddeus,Ankeny,Roseville,CA,95678";
         yield return "Lai,Harabedian,Novato,CA,94945";
         yield return "Harrison,Haufler,New Haven,CT,06515";
         yield return "Ma,Layous,North Haven,CT,06473";
         yield return "Zona,Colla,Norwalk,CT,06854";
         yield return "Teddy,Pedrozo,Bridgeport,CT,06610";
         yield return "Alesia,Hixenbaugh,Washington,DC,20001";
         yield return "Hoa,Sarao,Oak Hill,FL,32759";
         yield return "Joanna,Leinenbach,Lake Worth,FL,33461";
         yield return "Jeanice,Claucherty,Miami,FL,33142";
         yield return "Sharika,Eanes,Orlando,FL,32806";
         yield return "Nickolas,Juvera,Crystal River,FL,34429";
         yield return "Shawnda,Yori,Longwood,FL,32750";
         yield return "Jovita,Oles,Daytona Beach,FL,32114";
         yield return "Raymon,Calvaresi,Indianapolis,IN,46222";
         yield return "Beatriz,Corrington,Middleboro,MA,02346";
         yield return "Annabelle,Boord,Concord,MA,01742";
         yield return "Lashaunda,Lizama,Hanover,MD,21076";
         yield return "Elouise,Gwalthney,Bladensburg,MD,20710";
         yield return "Annelle,Tagala,Parkville,MD,21234";
         yield return "Markus,Lukasik,Sterling Heights,MI,48310";
         yield return "Sharee,Maile,Muskegon,MI,49442";
         yield return "Vilma,Berlanga,Grand Rapids,MI,49546";
         yield return "Quentin,Swayze,Milan,MI,48160";
         yield return "Kenneth,Grenet,East Lansing,MI,48823";
         yield return "Albina,Glick,Dunellen,NJ,08812";
         yield return "Karl,Klonowski,Flemington,NJ,08822";
         yield return "Jamal,Vanausdal,Monroe Township,NJ,08831";
         yield return "Delisa,Crupi,Newark,NJ,07105";
         yield return "Elza,Lipke,Newark,NJ,07104";
         yield return "Rolland,Francescon,Paterson,NJ,07501";
         yield return "Ty,Smith,Hackensack,NJ,07601";
         yield return "Thurman,Manno,Absecon,NJ,08201";
         yield return "Becky,Mirafuentes,Plainfield,NJ,07062";
         yield return "Cheryl,Haroldson,Atlantic City,NJ,08401";
         yield return "Lizette,Stem,Cherry Hill,NJ,08002";
         yield return "Irma,Wolfgramm,Randolph,NJ,07869";
         yield return "Nelida,Sawchuk,Paramus,NJ,07652";
         yield return "Junita,Brideau,Cedar Grove,NJ,07009";
         yield return "Merlyn,Lawler,Jersey City,NJ,07304";
         yield return "Jettie,Mconnell,Bridgewater,NJ,08807";
         yield return "Candida,Corbley,Somerville,NJ,08876";
         yield return "Herman,Demesa,Troy,NY,12180";
         yield return "Bok,Isaacs,Bronx,NY,10468";
         yield return "Theola,Frey,Massapequa,NY,11758";
         yield return "Ozell,Shealy,New York,NY,10002";
         yield return "Yolando,Luczki,Syracuse,NY,13214";
         yield return "Gregoria,Pawlowicz,Garden City,NY,11530";
         yield return "Latrice,Tolfree,Ronkonkoma,NY,11779";
         yield return "Nana,Wrinkles,Mount Vernon,NY,10553";
         yield return "Jesusita,Flister,Lancaster,PA,17601";
         yield return "Jennie,Drymon,Scranton,PA,18509";
         yield return "Dalene,Schoeneck,Philadelphia,PA,19102";
         yield return "Lashandra,Klang,King of Prussia,PA,19406";
         yield return "Ahmed,Angalich,Harrisburg,PA,17110";
      }

      //Recorder of events fired by Data Conveyer, so that they can be evaluated (asserted on) upon completion.
      private readonly EventRecorder _eventRecorder;

      //Executor of a series of asserts; in case assert faile, a text file (or files) is/are saved for manual examination.
      private readonly TraceableAsserter<EvtInstance> _traceableAsserter;

      public EtlOrchestrator_tests_Counts()
      {
         _config = new OrchestratorConfig
         {
            ReportProgress = true,
            InputDataKind = KindOfTextData.Delimited
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.MarkerStartsCluster = true;
         _config.ConcurrencyLevel = 4;
         _config.PhaseStartingHandler = (s, e) => _eventRecorder.RecordEvent(e.Phase, EvtType.Starting, e.RecCnt, e.ClstrCnt, 0, 0);
         _config.ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Transformation && e.ClstrCnt == 4) Thread.Sleep(1); _eventRecorder.RecordEvent(e.Phase, EvtType.Progress, e.RecCnt, e.ClstrCnt, e.RecNo, e.ClstrNo); };
         // note the delay added in ProgressChangedHandler; this is to force Transformation processing in non-linear order (but only in case of parallelization, which does not apply in this test)
         _config.PhaseFinishedHandler = (s, e) => _eventRecorder.RecordEvent(e.Phase, EvtType.Finished, e.RecCnt, e.ClstrCnt, 0, 0);

         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputConsumer = (t, gc) => { };   // throwaway consumer; resulting output is of no relevance to the tests in this class

         _eventRecorder = new EventRecorder();

         // AsserterOutput tuple: Item1=Ext, Item2=Header, Item3=Formatter, Item4=ExcFormatter
         AsserterOutput asserterOutputToTxt = (".txt",
                                                "SeqNo Phase          EvtType  PrgSeqNo RecCnt ClstrCnt RecNo ClstrNo ThreadId",
                                                e => $"{e.SeqNo,4}  { e.Phase,-14} { e.EvtType,-8} { e.PrgSeqNo,7 } { e.RecCnt,6 } { e.ClstrCnt,8 } { e.RecNo,5 } { e.ClstrNo,7} { e.ThreadId,8}",
                                                ex => ex.ToString().Split("\r\n")
                                              );
         AsserterOutput asserterOutputToCsv = (".csv",
                                                "SeqNo,Phase,EvtType,PrgSeqNo,RecCnt,ClstrCnt,RecNo,ClstrNo,ThreadId",
                                                e => $"{e.SeqNo},{ e.Phase},{ e.EvtType},{ e.PrgSeqNo },{ e.RecCnt },{ e.ClstrCnt },{ e.RecNo },{ e.ClstrNo},{ e.ThreadId}",
                                                ex => Enumerable.Repeat("\"" + ex.Message + "\"", 1)
                                              );
         _traceableAsserter = new TraceableAsserter<EvtInstance>("EventTestFailures\\", asserterOutputToTxt, asserterOutputToCsv);
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_HeadersBothInAndOut_CorrectCounts(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ProgressInterval = 1;
         _config.HeadersInFirstInputRow = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state
            if (prevRec == null) return true;
            return (string)rec["STATE"] != (string)prevRec["STATE"];
         };
         _config.HeadersInFirstOutputRow = true;
         _config.OutputFields = "LNAME|20,STATE|2,ZIP";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(68);    //1 header row + 67 data rows
         counts.ClustersRead.Should().Be(13);   //13 states
         counts.ClustersWritten.Should().Be(13);
         counts.RowsWritten.Should().Be(68);

         var events = _eventRecorder.GetEventList();

         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ProcessPipeline_HeadersBothInAndOut_CorrectCounts) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            events, allEvents =>
         {
            allEvents.Should().HaveCount(45);  // (1 starting + 13 progress change + 1 finished) * 3
            allEvents.Min(e => e.SeqNo).Should().Be(1);
            allEvents.Max(e => e.SeqNo).Should().Be(45);

            //first
            allEvents[0].Phase.Should().Be(Phase.Intake);
            allEvents[0].EvtType.Should().Be(EvtType.Starting);
            allEvents[0].PrgSeqNo.Should().Be(0);
            allEvents[0].RecCnt.Should().Be(0);
            allEvents[0].ClstrCnt.Should().Be(0);
            allEvents[0].RecNo.Should().Be(0);
            allEvents[0].ClstrNo.Should().Be(0);

            //last
            allEvents[44].Phase.Should().Be(Phase.Output);
            allEvents[44].EvtType.Should().Be(EvtType.Finished);
            allEvents[44].PrgSeqNo.Should().Be(0);
            allEvents[44].RecCnt.Should().Be(67);
            allEvents[44].ClstrCnt.Should().Be(13);
            allEvents[44].RecNo.Should().Be(0);
            allEvents[44].ClstrNo.Should().Be(0);

            var strtEvents = allEvents.Where(e => e.EvtType == EvtType.Starting).ToList();
            strtEvents.Should().HaveCount(3);
            strtEvents.Select(e => e.Phase).Should().Contain(new List<Phase> { Phase.Intake, Phase.Transformation, Phase.Output }); //note that ContainInOrder may fail (Output start may fire before Transformation)
            strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

            var prgEvents = allEvents.Where(e => e.EvtType == EvtType.Progress).ToList();
            prgEvents.Should().HaveCount(39);  // 13 + 13 + 13
            prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);

            var prgEventsIn = prgEvents.Where(e => e.Phase == Phase.Intake).ToList();
            prgEventsIn.Should().HaveCount(13); //progress reported on every one of the 13 clusters
            prgEventsIn.Select(e => e.ClstrNo).Should().BeInAscendingOrder();
            prgEventsIn[0].ClstrCnt.Should().Be(1);   // 1st clstr (AR)
            prgEventsIn[0].RecCnt.Should().Be(1);     // AR ends at rec 1 (excl. hdr)
            prgEventsIn[2].ClstrCnt.Should().Be(3);   // clstr 3 (CA)
            prgEventsIn[2].RecCnt.Should().Be(14);    // CA ends at rec 14  (excl. hdr)
            prgEventsIn[8].ClstrCnt.Should().Be(9);   // clstr 9 (MD)
            prgEventsIn[8].RecCnt.Should().Be(32);    // MD ends at rec 32 (excl. hdr)
            prgEventsIn[12].ClstrCnt.Should().Be(13);  // last clstr (PA)
            prgEventsIn[12].RecCnt.Should().Be(67);    // PA ends at rec 67 (excl. hdr)

            var prgEventsTr = prgEvents.Where(e => e.Phase == Phase.Transformation).ToList();
            prgEventsTr.Should().HaveCount(13);
            // Progress events for Transformation are by all likelihood not in ascending order by ClstrNo (due to parallelization).
            // So, we're only verifying ClstrCnt increments of 2. Note that both RecNo and RecCnt vary depending order events are occurring.
            var prgEventsTrClstrCnts = prgEventsTr.Select(e => e.ClstrCnt).ToList();
            prgEventsTrClstrCnts.Should().OnlyHaveUniqueItems();

            prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(13);
            prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

            var finEvents = allEvents.Where(e => e.EvtType == EvtType.Finished);
            finEvents.Should().HaveCount(3);
            finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
            finEvents.Should().OnlyContain(e => e.RecCnt == 67 && e.ClstrCnt == 13 && e.RecNo == 0 && e.ClstrNo == 0);

            allEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(15);  // 1 + 14 + 1
            allEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(15);
            allEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(15);
         });
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_NoHeadersEitherInOrOut_CorrectCounts(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.ProgressInterval = 0;  //only Starting & Finished events
         _config.HeadersInFirstInputRow = false;
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state (using default field names)
            if (prevRec == null) return true;
            return (string)rec["Fld004"] != (string)prevRec["Fld004"];  //default field names (header row is interpreted as data row)
         };
         _config.HeadersInFirstOutputRow = false;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";

         _config.RecordboundTransformer = r => { Thread.Sleep(1); return r; };  //delay to allow Output phase start

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(68);    //all 68 rows assumed to be data rows
         counts.ClustersRead.Should().Be(14);   //13 states + the word STATE from 1st row
         counts.ClustersWritten.Should().Be(14);
         counts.RowsWritten.Should().Be(68);

         var allEvents = _eventRecorder.GetEventList();

         allEvents.Should().HaveCount(6);  // (1 starting + 1 finished) * 3
         allEvents.Min(e => e.SeqNo).Should().Be(1);
         allEvents.Max(e => e.SeqNo).Should().Be(6);

         //first
         allEvents[0].Phase.Should().Be(Phase.Intake);
         allEvents[0].EvtType.Should().Be(EvtType.Starting);
         allEvents[0].PrgSeqNo.Should().Be(0);
         allEvents[0].RecCnt.Should().Be(0);
         allEvents[0].ClstrCnt.Should().Be(0);
         allEvents[0].RecNo.Should().Be(0);
         allEvents[0].ClstrNo.Should().Be(0);

         //last
         allEvents[5].Phase.Should().Be(Phase.Output);
         allEvents[5].EvtType.Should().Be(EvtType.Finished);
         allEvents[5].PrgSeqNo.Should().Be(0);
         allEvents[5].RecCnt.Should().Be(68);
         allEvents[5].ClstrCnt.Should().Be(14);
         allEvents[5].RecNo.Should().Be(0);
         allEvents[5].ClstrNo.Should().Be(0);

         var strtEvents = allEvents.Where(e => e.EvtType == EvtType.Starting).ToList();
         strtEvents.Should().HaveCount(3);
         strtEvents.Select(e => e.Phase).Should().Contain(new List<Phase> { Phase.Intake, Phase.Transformation, Phase.Output }); //note that ContainInOrder may fail (Output start may fire before Transformation)
         strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

         var prgEvents = allEvents.Where(e => e.EvtType == EvtType.Progress).ToList();
         prgEvents.Should().BeEmpty();

         var finEvents = allEvents.Where(e => e.EvtType == EvtType.Finished);
         finEvents.Should().HaveCount(3);
         finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
         finEvents.Should().OnlyContain(e => e.RecCnt == 68 && e.ClstrCnt == 14 && e.RecNo == 0 && e.ClstrNo == 0);

         allEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(2);
         allEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(2);
         allEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(2);
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_NoInHdrsButOutHdrPlusLdrAndTrlr_CorrectCounts(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ProgressInterval = 1;  //every cluster
         _config.HeadersInFirstInputRow = false;
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state (using default field names)
            if (prevRec == null) return true;
            return (string)rec["Fld004"] != (string)prevRec["Fld004"];  //default field names (header row is interpreted as data row)
         };
         _config.HeadersInFirstOutputRow = true;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";
         _config.LeaderContents = "some\r\nleader";  //2 lines
         _config.TrailerContents = "some trailer";  //single line

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(68);        //all 68 rows assumed to be data rows
         counts.ClustersRead.Should().Be(14);    //13 states + the word STATE from 1st row
         counts.ClustersWritten.Should().Be(14);
         counts.RowsWritten.Should().Be(72);     //1 hdr row + 2 leader + 68 data rows + 1 trailer

         var events = _eventRecorder.GetEventList();

         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ProcessPipeline_NoInHdrsButOutHdrPlusLdrAndTrlr_CorrectCounts) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            events, allEvents =>
         {
            allEvents.Should().HaveCount(48);  // (1 starting + 14 progress change + 1 finished) * 3
            allEvents.Min(e => e.SeqNo).Should().Be(1);
            allEvents.Max(e => e.SeqNo).Should().Be(48);

            //first
            allEvents[0].Phase.Should().Be(Phase.Intake);
            allEvents[0].EvtType.Should().Be(EvtType.Starting);
            allEvents[0].PrgSeqNo.Should().Be(0);
            allEvents[0].RecCnt.Should().Be(0);
            allEvents[0].ClstrCnt.Should().Be(0);
            allEvents[0].RecNo.Should().Be(0);
            allEvents[0].ClstrNo.Should().Be(0);

            //last
            allEvents[47].Phase.Should().Be(Phase.Output);
            allEvents[47].EvtType.Should().Be(EvtType.Finished);
            allEvents[47].PrgSeqNo.Should().Be(0);
            allEvents[47].RecCnt.Should().Be(68);
            allEvents[47].ClstrCnt.Should().Be(14);
            allEvents[47].RecNo.Should().Be(0);
            allEvents[47].ClstrNo.Should().Be(0);

            var strtEvents = allEvents.Where(e => e.EvtType == EvtType.Starting).ToList();
            strtEvents.Should().HaveCount(3);
            strtEvents.Select(e => e.Phase).Should().Contain(new List<Phase>{ Phase.Intake, Phase.Transformation, Phase.Output } ); //note that ContainInOrder may fail (Output start may fire before Transformation)
            strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0); 

            var prgEvents = allEvents.Where(e => e.EvtType == EvtType.Progress).ToList();
            prgEvents.Should().HaveCount(42);  // 14 + 14 + 14
            prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);

            var prgEventsIn = prgEvents.Where(e => e.Phase == Phase.Intake).ToList();
            prgEventsIn.Should().HaveCount(14); //progress reported on every one of 14 clusters
            prgEventsIn.Select(e => e.ClstrNo).Should().BeInAscendingOrder();
            prgEventsIn[0].ClstrCnt.Should().Be(1);   // 1st clstr is the one with Fld004 = STATE
            prgEventsIn[0].RecCnt.Should().Be(1);     // this 1st clstr is only 1 record
            prgEventsIn[1].ClstrCnt.Should().Be(2);   // 2nd clstr (AR)
            prgEventsIn[1].RecCnt.Should().Be(2);     // AR ends at rec 2
            prgEventsIn[3].ClstrCnt.Should().Be(4);   // clstr 4 (CA)
            prgEventsIn[3].RecCnt.Should().Be(15);    // CA ends at rec 15
            prgEventsIn[9].ClstrCnt.Should().Be(10);  // clstr 10 (MD)
            prgEventsIn[9].RecCnt.Should().Be(33);    // MD ends at rec 33
            prgEventsIn[13].ClstrCnt.Should().Be(14);  // last clstr (PA)
            prgEventsIn[13].RecCnt.Should().Be(68);    // PA ends at rec 68

            var prgEventsTr = prgEvents.Where(e => e.Phase == Phase.Transformation).ToList();
            prgEventsTr.Should().HaveCount(14);
            // Progress events for Transformation are by all likelihood not in ascending order by ClstrNo (due to parallelization).
            // So, we're only verifying ClstrCnt increments of 2. Note that both RecNo and RecCnt vary depending order events are occurring.
            var prgEventsTrClstrCnts = prgEventsTr.Select(e => e.ClstrCnt).ToList();
            prgEventsTrClstrCnts.Should().OnlyHaveUniqueItems();

            prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(14);
            prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

            var finEvents = allEvents.Where(e => e.EvtType == EvtType.Finished);
            finEvents.Should().HaveCount(3);
            finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
            finEvents.Should().OnlyContain(e => e.RecCnt == 68 && e.ClstrCnt == 14 && e.RecNo == 0 && e.ClstrNo == 0);
            // Note that even if e.Phase == Phase.Output then RecCnt on Output only reports data rows (no header row, leader or trailer), i.e. is the same as for Intake and Transformation.
            // This is unlike ProcessResult.RowsOut, which reports all output rows, including header row, leader and trailer.

            allEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(16);  // 1 + 14 + 1
            allEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(16);
            allEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(16);
         });
      }


      [Theory]
      [Repeat(10)]
      public void ProcessPipeline_NoInHdrsButOutHdrPlusLdrAndTrlrEveryOtherClstr_CorrectCounts(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ProgressInterval = 2;  //every other cluster
         _config.HeadersInFirstInputRow = false;
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state (using default field names)
            if (prevRec == null) return true;
            return (string)rec["Fld004"] != (string)prevRec["Fld004"];  //default field names (header row is interpreted as data row)
         };
         _config.HeadersInFirstOutputRow = true;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";
         _config.LeaderContents = "some\r\nleader";  //2 lines
         _config.TrailerContents = "some trailer";  //single line

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(68);        //all 68 rows assumed to be data rows
         counts.ClustersRead.Should().Be(14);    //13 states + the word STATE from 1st row
         counts.ClustersWritten.Should().Be(14);
         counts.RowsWritten.Should().Be(72);     //1 hdr row + 2 leader + 68 data rows + 1 trailer

         var events = _eventRecorder.GetEventList();

         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ProcessPipeline_NoInHdrsButOutHdrPlusLdrAndTrlrEveryOtherClstr_CorrectCounts) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            events, allEvents =>
         {
            allEvents.Should().HaveCount(27);  // (1 starting + 7 progress change + 1 finished) * 3
            allEvents.Min(e => e.SeqNo).Should().Be(1);
            allEvents.Max(e => e.SeqNo).Should().Be(27);

            //first
            allEvents[0].Phase.Should().Be(Phase.Intake);
            allEvents[0].EvtType.Should().Be(EvtType.Starting);
            allEvents[0].PrgSeqNo.Should().Be(0);
            allEvents[0].RecCnt.Should().Be(0);
            allEvents[0].ClstrCnt.Should().Be(0);
            allEvents[0].RecNo.Should().Be(0);
            allEvents[0].ClstrNo.Should().Be(0);

            //last
            allEvents[26].Phase.Should().Be(Phase.Output);
            allEvents[26].EvtType.Should().Be(EvtType.Finished);
            allEvents[26].PrgSeqNo.Should().Be(0);
            allEvents[26].RecCnt.Should().Be(68);
            allEvents[26].ClstrCnt.Should().Be(14);
            allEvents[26].RecNo.Should().Be(0);
            allEvents[26].ClstrNo.Should().Be(0);

            var strtEvents = allEvents.Where(e => e.EvtType == EvtType.Starting).ToList();
            strtEvents.Should().HaveCount(3);
            strtEvents.Select(e => e.Phase).Should().Contain(new List<Phase> { Phase.Intake, Phase.Transformation, Phase.Output }); //note that ContainInOrder may fail (Output start may fire before Transformation)
            strtEvents.Should().OnlyContain(e => e.RecCnt == 0 && e.ClstrCnt == 0 && e.RecNo == 0 && e.ClstrNo == 0);

            var prgEvents = allEvents.Where(e => e.EvtType == EvtType.Progress).ToList();
            prgEvents.Should().HaveCount(21);  // 7 + 7 + 7 (every other of the 14 clusters)
            prgEvents.Should().OnlyContain(e => e.PrgSeqNo > 0);
            prgEvents.Should().OnlyContain(e => e.ClstrCnt % 2 == 0);  //only even clusters

            var prgEventsIn = prgEvents.Where(e => e.Phase == Phase.Intake).ToList();
            prgEventsIn.Should().HaveCount(7); //progress reported on every other one of 14 clusters
            prgEventsIn.Select(e => e.ClstrNo).Should().BeInAscendingOrder();
            prgEventsIn[0].ClstrCnt.Should().Be(2);   // 1st reported is 2nd clstr(AR) (note that 1st clstr was the one with Fld004 = STATE )
            prgEventsIn[0].RecCnt.Should().Be(2);     // AR ends at rec 2
            prgEventsIn[1].ClstrCnt.Should().Be(4);   // 2nd is clstr 4 (CA)
            prgEventsIn[1].RecCnt.Should().Be(15);    // CA ends at rec 15
            prgEventsIn[4].ClstrCnt.Should().Be(10);  // clstr 10 (MD)
            prgEventsIn[4].RecCnt.Should().Be(33);    // MD ends at rec 33
            prgEventsIn[6].ClstrCnt.Should().Be(14);  // last clstr (PA)
            prgEventsIn[6].RecCnt.Should().Be(68);    // PA ends at rec 68

            var prgEventsTr = prgEvents.Where(e => e.Phase == Phase.Transformation).ToList();
            prgEventsTr.Should().HaveCount(7);
            // Progress events for Transformation are by all likelihood not in ascending order by ClstrNo (due to parallelization).
            // So, we're only verifying ClstrCnt increments of 2. Note that both RecNo and RecCnt vary depending order events are occurring.
            var prgEventsTrClstrCnts = prgEventsTr.Select(e => e.ClstrCnt).ToList();
            prgEventsTrClstrCnts.Should().OnlyHaveUniqueItems();
            prgEventsTrClstrCnts.ForEach(cc => cc.Should().BeOneOf(2, 4, 6, 8, 10, 12, 14));

            prgEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(7);
            prgEvents.Where(e => e.Phase == Phase.Output).Select(e => e.ClstrNo).Should().BeInAscendingOrder();

            var finEvents = allEvents.Where(e => e.EvtType == EvtType.Finished);
            finEvents.Should().HaveCount(3);
            finEvents.Select(e => e.Phase).Should().ContainInOrder(Phase.Intake, Phase.Transformation, Phase.Output);
            finEvents.Should().OnlyContain(e => e.RecCnt == 68 && e.ClstrCnt == 14 && e.RecNo == 0 && e.ClstrNo == 0);
            // Note that even if e.Phase == Phase.Output then RecCnt on Output only reports data rows (no header row, leader or trailer), i.e. is the same as for Intake and Transformation.
            // This is unlike ProcessResult.RowsOut, which reports all output rows, including header row, leader and trailer.

            allEvents.Where(e => e.Phase == Phase.Intake).Should().HaveCount(9);  // 1 + 7 + 1
            allEvents.Where(e => e.Phase == Phase.Transformation).Should().HaveCount(9);
            allEvents.Where(e => e.Phase == Phase.Output).Should().HaveCount(9);
         });
      }


      [Theory]
      [Repeat(10)]
      public void GetProcessingStatus_NoHeaders_CorrectPhaseStatus(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _recordedPhaseStatus = new ConcurrentDictionary<Tuple<Phase, HandlerType>, ConcurrentQueue<PhaseStatus>>();

         _config.ProgressInterval = 1;  //every cluster
         _config.SetIntakeSupplier(new IntakeSupplierProvider(Enumerable.Repeat(_intakeLines(), 10).SelectMany(l => l)).StringSupplier); //repeat input 10 times for a total of 680 lines
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state (using default field names)
            RecordCountsEvery10th(HandlerType.ClstrMarker, rec, 680);
            if (prevRec == null) return true;
            return (string)rec["Fld004"] != (string)prevRec["Fld004"];  //default field names (header row is interpreted as data row)
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r => { Thread.Sleep(1); RecordCountsEvery10th(HandlerType.Transformer, r, 680); return r; };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) => { RecordCountsEvery10th(HandlerType.Router, r, 680); return 1; };
         _config.HeadersInFirstOutputRow = false;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(680);    // 68 rows 10 times
         counts.ClustersRead.Should().Be(140);
         counts.ClustersWritten.Should().Be(140);
         counts.RowsWritten.Should().Be(680);

         // recordedPhaseStatus is a local copy of _recordedPhaseStatus (concurrency removed to ease out asserts below)
         var recordedPhaseStatus = new Dictionary<Tuple<Phase, HandlerType>, List<PhaseStatus>>(_recordedPhaseStatus.Select(kvp => KeyValuePair.Create(kvp.Key, kvp.Value.ToList())));

         recordedPhaseStatus.Count.Should().Be(9);  // 3 phases * 3 handlers

         recordedPhaseStatus[IntakeAtClstrMarker].Count.Should().Be(69);  //recs: 1, 11, 21, .. ,  671, 680
         recordedPhaseStatus[IntakeAtClstrMarker][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtClstrMarker][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[IntakeAtClstrMarker][0].RecordCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][0].ClusterCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][68].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[TransformationAtClstrMarker].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtClstrMarker][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[TransformationAtClstrMarker][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtClstrMarker].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtClstrMarker][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[OutputAtClstrMarker][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[IntakeAtTransformer][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtTransformer][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtTransformer][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[TransformationAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtTransformer][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtTransformer][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[TransformationAtTransformer][68].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtTransformer][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtTransformer][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[IntakeAtRouter][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtRouter][68].State.Should().Be(ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtRouter][68].ClusterCount.Should().Be(140);
         recordedPhaseStatus[IntakeAtRouter][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[TransformationAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtRouter][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
                                                                                                                                                                  //       _recordedPhaseStatus[TransformationRouter][68].State.Should().Be(ExecutionState.Complete);  //last record - State may not get updated yet as it waits for completion of TransformationBlock, however counts should be complete
         recordedPhaseStatus[TransformationAtRouter][68].ClusterCount.Should().Be(140);
         recordedPhaseStatus[TransformationAtRouter][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[OutputAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtRouter][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtRouter][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtRouter][68].State.Should().Be(ExecutionState.Running);  //last record
         recordedPhaseStatus[OutputAtRouter][68].ClusterCount.Should().Be(140);
         recordedPhaseStatus[OutputAtRouter][68].RecordCount.Should().Be(680);
      }


      [Fact]
      public void GetProcessingStatus_EmptyHeadAndFootClusters_CorrecStatusesAndCounts()
      {
         //arrange
         _recordedPhaseStatus = new ConcurrentDictionary<Tuple<Phase, HandlerType>, ConcurrentQueue<PhaseStatus>>();

         _config.ProgressInterval = 1;  //every cluster
         _config.SetIntakeSupplier(new IntakeSupplierProvider(Enumerable.Repeat(_intakeLines(), 10).SelectMany(l => l)).StringSupplier); //repeat input 10 times for a total of 680 lines
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.PrependHeadCluster = true;
         _config.AppendFootCluster = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state (using default field names)
            RecordCountsEvery10th(HandlerType.ClstrMarker, rec, 680);
            if (prevRec == null) return true;
            return (string)rec["Fld004"] != (string)prevRec["Fld004"];  //default field names (header row is interpreted as data row)
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r => { Thread.Sleep(1); RecordCountsEvery10th(HandlerType.Transformer, r, 680); return r; };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) => { RecordCountsEvery10th(HandlerType.Router, r, 680); return 1; };
         _config.HeadersInFirstOutputRow = false;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";
         _config.LeaderContents = "Leading Line";
         _config.TrailerContents = "Trailing Line1\r\nTrailing Line2";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(680);        // 68 rows 10 times
         counts.ClustersRead.Should().Be(142);    // 140 + HeadCluster + FootCluster (note that they both count even though they're not read from intake)
         counts.ClustersWritten.Should().Be(140); // both head and foot clusters were empty and hence "swallowed" (removed) during transformation
         counts.RowsWritten.Should().Be(683);     // 680 + 1 leader line + 2 trailer lines

         // recordedPhaseStatus is a local copy of _recordedPhaseStatus (concurrency removed to ease out asserts below)
         var recordedPhaseStatus = new Dictionary<Tuple<Phase, HandlerType>, List<PhaseStatus>>(_recordedPhaseStatus.Select(kvp => KeyValuePair.Create(kvp.Key, kvp.Value.ToList())));

         recordedPhaseStatus.Count.Should().Be(9);  // 3 * 3, ie.e status of 3 phases (Intake, Transformation, Output) at 3 handlers (ClstrMarker, Transformer, Router)

         recordedPhaseStatus[IntakeAtClstrMarker].Count.Should().Be(69);  //recs: 1, 11, 21, .. ,  671, 680
         recordedPhaseStatus[IntakeAtClstrMarker][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtClstrMarker][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[IntakeAtClstrMarker][0].RecordCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][0].ClusterCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][68].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[TransformationAtClstrMarker].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtClstrMarker][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[TransformationAtClstrMarker][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtClstrMarker].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtClstrMarker][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[OutputAtClstrMarker][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[IntakeAtTransformer][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtTransformer][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtTransformer][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[TransformationAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtTransformer][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtTransformer][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[TransformationAtTransformer][68].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtTransformer].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtTransformer][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtTransformer][68].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[IntakeAtRouter][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtRouter][68].State.Should().Be(ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtRouter][68].ClusterCount.Should().Be(142);  // both head and foot clusters are present at Intake
         recordedPhaseStatus[IntakeAtRouter][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[TransformationAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[TransformationAtRouter][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record                                                                                                                                                                   //       _recordedPhaseStatus[TransformationRouter][68].State.Should().Be(ExecutionState.Complete);  //last record - State may not get updated yet as it waits for completion of TransformationBlock, however counts should be complete
         recordedPhaseStatus[TransformationAtRouter][68].ClusterCount.Should().BeOneOf(141, 142);
         //Note that the last record (#680) is likely routed before the foot cluster (with no records) is transformed. In this case,
         // the ClusterCount (above) will be 141 (and not the final 142). Also note that the RecordCount (below) will be 680 before or after transforming the (empty) foot cluster.
         recordedPhaseStatus[TransformationAtRouter][68].RecordCount.Should().Be(680);

         recordedPhaseStatus[OutputAtRouter].Count.Should().Be(69);
         recordedPhaseStatus[OutputAtRouter][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtRouter][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtRouter][68].State.Should().Be(ExecutionState.Running);  //last record
         recordedPhaseStatus[OutputAtRouter][68].ClusterCount.Should().Be(140);  // both head and foot clusters have been "swallowed" (removed) during transformation
         recordedPhaseStatus[OutputAtRouter][68].RecordCount.Should().Be(680);
      }


      [Fact]
      public void GetProcessingStatus_LoadedHeadAndFootClusters_CorrecStatusesAndCounts()
      {
         //arrange
         _recordedPhaseStatus = new ConcurrentDictionary<Tuple<Phase, HandlerType>, ConcurrentQueue<PhaseStatus>>();

         _config.ProgressInterval = 1;  //every cluster
         //this intake supplier repeats input 10 times skipping header row for all repeats except first (for a total of 68 + 9*67 = 671 rows)
         _config.SetIntakeSupplier(new IntakeSupplierProvider(Enumerable.Concat(_intakeLines(), Enumerable.Repeat(_intakeLines().Skip(1), 9).SelectMany(l => l))).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.AllowOnTheFlyInputFields = true;  //to allow default field names
         _config.PrependHeadCluster = true;
         _config.AppendFootCluster = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {  //group clusters by state
            RecordCountsEvery10th(HandlerType.ClstrMarker, rec, 670);
            if (prevRec == null) return true;
            return (string)rec["STATE"] != (string)prevRec["STATE"];
         };
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = clstr =>
         {
            if (clstr.StartRecNo == Constants.HeadClusterRecNo)
            {  // add 3 dummy records to head cluster
               var rec = clstr.ObtainEmptyRecord();
               rec.AddItem("dummy", "head");
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               return clstr;
            }

            if (clstr.StartRecNo == Constants.FootClusterRecNo)
            {  // add 5 dummy records to foot cluster
               var rec = clstr.ObtainEmptyRecord();
               rec.AddItem("dummy", "foot");
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               clstr.AddRecord(rec);
               return clstr;
            }

            //regular cluster
            foreach (var rec in clstr.Records)
            {
               Thread.Sleep(1);
               RecordCountsEvery10th(HandlerType.Transformer, rec, 670);
            }
            return clstr;
         };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) => { RecordCountsEvery10th(HandlerType.Router, r, 670); return 1; };
         _config.HeadersInFirstOutputRow = false;
         _config.OutputFields = "Fld001|20,Fld004|2,Fld005";
         _config.LeaderContents = "Leading Line";
         _config.TrailerContents = "Trailing Line1\r\nTrailing Line2";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(671);        // 68 rows + 9 * 67
         counts.ClustersRead.Should().Be(132);    // 13*10 + HeadCluster + FootCluster (note that they both count even though they're not read from intake)
         counts.ClustersWritten.Should().Be(132);
         counts.RowsWritten.Should().Be(681);     // 67*10 + 1 leader line + 2 trailer lines + 3 HeadCluster records + 5 FootCluster records

         // recordedPhaseStatus is a local copy of _recordedPhaseStatus (concurrency removed to ease out asserts below)
         var recordedPhaseStatus = new Dictionary<Tuple<Phase, HandlerType>, List<PhaseStatus>>(_recordedPhaseStatus.Select(kvp => KeyValuePair.Create(kvp.Key, kvp.Value.ToList())));

         recordedPhaseStatus.Count.Should().Be(9);  // 3 * 3, ie.e status of 3 phases (Intake, Transformation, Output) at 3 handlers (ClstrMarker, Transformer, Router)

         recordedPhaseStatus[IntakeAtClstrMarker].Count.Should().Be(68);  //recs: 1, 11, 21, .. ,  661, 670
         recordedPhaseStatus[IntakeAtClstrMarker][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtClstrMarker][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[IntakeAtClstrMarker][0].RecordCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][0].ClusterCount.Should().Be(0);
         recordedPhaseStatus[IntakeAtClstrMarker][67].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[TransformationAtClstrMarker].Count.Should().Be(68);
         recordedPhaseStatus[TransformationAtClstrMarker][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[TransformationAtClstrMarker][67].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtClstrMarker].Count.Should().Be(68);
         recordedPhaseStatus[OutputAtClstrMarker][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtClstrMarker][0].State.Should().Be(ExecutionState.NotYetStarted);  //first record
         recordedPhaseStatus[OutputAtClstrMarker][67].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtTransformer].Count.Should().Be(68);
         recordedPhaseStatus[IntakeAtTransformer][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtTransformer][67].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtTransformer][67].RecordCount.Should().Be(670);

         recordedPhaseStatus[TransformationAtTransformer].Count.Should().Be(68);
         recordedPhaseStatus[TransformationAtTransformer][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtTransformer][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[TransformationAtTransformer][67].State.Should().Be(ExecutionState.Running);  //last record

         recordedPhaseStatus[OutputAtTransformer].Count.Should().Be(68);
         recordedPhaseStatus[OutputAtTransformer][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtTransformer][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtTransformer][67].State.Should().Match<ExecutionState>(st => st == ExecutionState.NotYetStarted || st == ExecutionState.Running);  //last record

         recordedPhaseStatus[IntakeAtRouter].Count.Should().Be(68);
         recordedPhaseStatus[IntakeAtRouter][0].Phase.Should().Be(Phase.Intake);
         recordedPhaseStatus[IntakeAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record
         recordedPhaseStatus[IntakeAtRouter][67].State.Should().Be(ExecutionState.Complete);  //last record
         recordedPhaseStatus[IntakeAtRouter][67].ClusterCount.Should().Be(132);  // both head and foot clusters are present at Intake
         recordedPhaseStatus[IntakeAtRouter][67].RecordCount.Should().Be(670);

         recordedPhaseStatus[TransformationAtRouter].Count.Should().Be(68);
         recordedPhaseStatus[TransformationAtRouter][0].Phase.Should().Be(Phase.Transformation);
         recordedPhaseStatus[TransformationAtRouter][0].State.Should().Match<ExecutionState>(st => st == ExecutionState.Running || st == ExecutionState.Complete);  //first record                                                                                                                                                                    //       _recordedPhaseStatus[TransformationRouter][68].State.Should().Be(ExecutionState.Complete);  //last record - State may not get updated yet as it waits for completion of TransformationBlock, however counts should be complete
         recordedPhaseStatus[TransformationAtRouter][67].ClusterCount.Should().BeOneOf(131, 132);
         //Note that the last record (#670) is likely routed before the foot cluster (with no records) is transformed. In this case,
         // the ClusterCount (above) will be 131 (and not final 132). Also note that the RecordCount can be 673 or 678 depending on sequence.
         recordedPhaseStatus[TransformationAtRouter][67].RecordCount.Should().BeOneOf(673, 678);  // 670 + 3 from head cluster (+ possibly 5 from foot cluster)

         recordedPhaseStatus[OutputAtRouter].Count.Should().Be(68);
         recordedPhaseStatus[OutputAtRouter][0].Phase.Should().Be(Phase.Output);
         recordedPhaseStatus[OutputAtRouter][0].State.Should().Be(ExecutionState.Running);  //first record
         recordedPhaseStatus[OutputAtRouter][67].State.Should().Be(ExecutionState.Running);  //last record
         recordedPhaseStatus[OutputAtRouter][67].ClusterCount.Should().Be(131);  // 130 + head cluster (foot cluster didn't get processed yet - note that we're not recording head/foot cluster in this test)
         recordedPhaseStatus[OutputAtRouter][67].RecordCount.Should().Be(673);   //670 + 3 from head cluster (foot cluster didn't get processed yet)
      }


      // Helper members to support tests for GetProcessingStatus method.
      // The concept here is somewhat similar to the one implemented by the EventRecorder class.
      //TODO: Consider making EventRecorder class generic, e.g. InstanceTracer<T>, where T is EvtInstance or HandlerInstance
      private enum HandlerType { ClstrMarker, Transformer, Router }
      private ConcurrentDictionary<Tuple<Phase, HandlerType>, ConcurrentQueue<PhaseStatus>> _recordedPhaseStatus;
      private void RecordCountsEvery10th(HandlerType handler, IRecord recToGetStatusFrom, int lastRecNo)
      {
         if (recToGetStatusFrom.RecNo % 10 != 1 && recToGetStatusFrom.RecNo != lastRecNo) return;  //only report for records 1, 11, .. , 61, .. plus the last record
         UpdateRecordedPhaseStatus(Phase.Intake, handler, recToGetStatusFrom.GetProcessingStatus(Phase.Intake));
         UpdateRecordedPhaseStatus(Phase.Transformation, handler, recToGetStatusFrom.GetProcessingStatus(Phase.Transformation));
         UpdateRecordedPhaseStatus(Phase.Output, handler, recToGetStatusFrom.GetProcessingStatus(Phase.Output));
      }
      private void UpdateRecordedPhaseStatus(Phase phase, HandlerType handler, PhaseStatus statusToReport)
      {
         _recordedPhaseStatus.AddOrUpdate(Tuple.Create(phase, handler),
                                            new ConcurrentQueue<PhaseStatus>(Enumerable.Repeat(statusToReport, 1)),
                                            (t, l) => { l.Enqueue(statusToReport); return l; });
      }
      //These "constant" tuples simply define a set of 9 possible keys to the _recordedPhaseStatus dictionary
      private readonly Tuple<Phase, HandlerType> IntakeAtClstrMarker = Tuple.Create(Phase.Intake, HandlerType.ClstrMarker);
      private readonly Tuple<Phase, HandlerType> TransformationAtClstrMarker = Tuple.Create(Phase.Transformation, HandlerType.ClstrMarker);
      private readonly Tuple<Phase, HandlerType> OutputAtClstrMarker = Tuple.Create(Phase.Output, HandlerType.ClstrMarker);
      private readonly Tuple<Phase, HandlerType> IntakeAtTransformer = Tuple.Create(Phase.Intake, HandlerType.Transformer);
      private readonly Tuple<Phase, HandlerType> TransformationAtTransformer = Tuple.Create(Phase.Transformation, HandlerType.Transformer);
      private readonly Tuple<Phase, HandlerType> OutputAtTransformer = Tuple.Create(Phase.Output, HandlerType.Transformer);
      private readonly Tuple<Phase, HandlerType> IntakeAtRouter = Tuple.Create(Phase.Intake, HandlerType.Router);
      private readonly Tuple<Phase, HandlerType> TransformationAtRouter = Tuple.Create(Phase.Transformation, HandlerType.Router);
      private readonly Tuple<Phase, HandlerType> OutputAtRouter = Tuple.Create(Phase.Output, HandlerType.Router);

   }
}
