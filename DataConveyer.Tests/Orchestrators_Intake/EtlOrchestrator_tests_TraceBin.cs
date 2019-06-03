//EtlOrchestrator_tests_TraceBin.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_TraceBin
   {
      private readonly OrchestratorConfig _config;
      private IEnumerable<string> _intakeLines()  //note 2 ISA envelopes with different delimiters
      {
         yield return "ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~\r";
         yield return "GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS";
         yield return "ST*834*0001";
         yield return "BGN*00*1234*20001227*0838*PT***2";
         yield return "N1*P5**FI*954529603";
         yield return "INS*Y*18*021*28*A*E**FT";
         yield return "REF*0F*123456789";
         yield return "REF*1L*G86W553";
         yield return "DTP*356*D8*20001220";
         yield return "NM1*IL*1*STEPHENS*MARIE*V***34*123456789";
         yield return "PER*IP**HP*4152296748*WP*4152968732";
         yield return "N3*\"123 MAIN STREET\"";  //to test RetainQuotes
         yield return "N4*SAN FRANCISCO*CA*94515";
         yield return "DMG*D8*19691017*F*M";
         yield return "HD*021**HLT*01A3*EMP";
         yield return "DTP*348*D8*20001220";
         yield return "LX*1";
         yield return "NM1*P3*1*FREDRICKSON*STEVE****XX*1234567891*25";
         yield return "SE*17*0001";
         yield return "GE*1*1421";
         yield return "IEA*1*455321165";
         yield return "ISA>00>          >01>SECRET    >ZZ>SUBMITTERS.ID  >ZZ>RECEIVERS.ID   >030101>1253>^>00501>000000905>1>T>:\r";
         yield return "GS>HC>SENDER CODE>RECEIVER CODE>19991231>0802>1>X>005010X222";
         yield return "ST>837>0021>005010X222";
         yield return "BHT>0019>00>244579>20061015>1023>CH";
         yield return "NM1>41>2>PREMIER BILLING SERVICE>>>>>46>TGJ23";
         yield return "PER>IC>JERRY>TE>3055552222>EX>231";
         yield return "NM1>40>2>KEY INSURANCE COMPANY>>>>>46>66783JJT";
         yield return "HL>1>>20>1";
         yield return "PRV>BI>PXC>203BF0100Y";
         yield return "NM1>85>2>BEN KILDARE SERVICE>>>>>XX>9876543210";
         yield return "N3>234 SEAWAY ST";
         yield return "N4>MIAMI>FL>33111";
         yield return "REF>EI>587654321";
         yield return "NM1>87>2";
         yield return "N3>2345 OCEAN BLVD";
         yield return "N4>MAIMI>FL>33111";
         yield return "HL>2>1>22>1";
         yield return "SBR>P>>2222-SJ>>>>>>CI";
         yield return "NM1>IL>1>SMITH>JANE>>>>MI>JS00111223333";
         yield return "DMG>D8>19430501>F";
         yield return "NM1>PR>2>KEY INSURANCE COMPANY>>>>>PI>999996666";
         yield return "REF>G2>KA6663";
         yield return "HL>3>2>23>0";
         yield return "PAT>19";
         yield return "NM1>QC>1>SMITH>TED";
         yield return "N3>236 N MAIN ST";
         yield return "N4>MIAMI>FL>33413";
         yield return "DMG>D8>19730501>M";
         yield return "CLM>26463774>100>>>11:B:1>Y>A>Y>I";
         yield return "REF>D9>17312345600006351";
         yield return "HI>BK:0340>BF:V7389";
         yield return "LX>1";
         yield return "SV1>HC:99213>40>UN>1>>>1";
         yield return "DTP>472>D8>20061003";
         yield return "LX>2";
         yield return "SV1>HC:87070>15>UN>1>>>1";
         yield return "DTP>472>D8>20061003";
         yield return "LX>3";
         yield return "SV1>HC:99214>35>UN>1>>>2";
         yield return "DTP>472>D8>20061010";
         yield return "LX>4";
         yield return "SV1>HC:86663>10>UN>1>>>2";
         yield return "DTP>472>D8>20061010";
         yield return "SE>42>0021";
         yield return "GE>1>1";
         yield return "IEA>1>000000905";
      }

      //Results of the tests (loaded at supplied functions) are held here:
      private readonly ConcurrentQueue<IReadOnlyDictionary<string, object>> _traceBinHistory;  //will contain results to verify

      public EtlOrchestrator_tests_TraceBin()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.X12
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         //no type definitions (everything string)

         _traceBinHistory = new ConcurrentQueue<IReadOnlyDictionary<string, object>>();
      }

      [Fact]
      public void RecordInitiator_X12TraceBinSetForISAandGS_CorrectContentsInRecTrfmr()
      {
         //arrange
         _config.RecordInitiator = (rec, tb) =>
         {
            switch (rec["Segment"])
            {
               case "ISA": tb.Add("ISA06", (string)rec["Elem006"]); break;
               case "IEA": tb.Clear(); break;
               case "GS": tb.Add("GS08", (string)rec["Elem008"]); break;
               case "GE": tb.Remove("GS08"); break;
            }
            return true;
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = rec =>
         {
            _traceBinHistory.Enqueue(rec.TraceBin);
            return rec;
         };
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(67);  //21 + 46
         results.ClustersRead.Should().Be(67);  //clustering is off (inconsistent with X12), but it's not the point of this test
         results.RowsWritten.Should().Be(67);
         results.ClustersWritten.Should().Be(67);

         var tbHistory = _traceBinHistory.ToList();

         tbHistory.Should().HaveCount(67);  // = record count (as created by recordbound transformer)

         var tBin = tbHistory[0];  //ISA
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("054318936      ");

         tBin = tbHistory[1];  //GS
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[2];  //ST
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[3];  //BGN
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[18];  //SE
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[19];  //GE
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("054318936      ");

         tBin = tbHistory[20];  //IEA
         tBin.Should().BeNull();

         tBin = tbHistory[21];  //ISA
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");

         tBin = tbHistory[22];  //GS
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[23];  //ST
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[24];  //BHT
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[34];  //NM1
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[64];  //SE
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[65];  //GE
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");

         tBin = tbHistory[66];  //IEA
         tBin.Should().BeNull();
      }

      [Fact]
      public void RecordInitiator_X12TraceBinSetNonStrings_CorrectContentsInRecTrfmr()
      {
         //This test will set and verify Tuples and int saved in the Trace Bin

         //arrange
         _config.RecordInitiator = (rec, tb) =>
         {
            switch (rec["Segment"])
            {
               case "ISA": tb.Add("int", 157); break;  // value of type int
               case "IEA": tb.Clear(); break;
               case "GS": tb.Add("GS05and08", Tuple.Create(int.Parse((string)rec[5]), (string)rec["Elem008"])); break; //value of type Tuple<int,string>
               case "GE": tb.Remove("GS05and08"); break;
            }
            return true;
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = rec =>
         {
            var outRec = rec.GetClone();  //cloning should retain the original TraceBin
            _traceBinHistory.Enqueue(outRec.TraceBin);
            return outRec;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(67);  //21 + 46
         results.ClustersRead.Should().Be(67);  //clustering is off (inconsistent with X12), but it's not the point of this test
         results.RowsWritten.Should().Be(67);
         results.ClustersWritten.Should().Be(67);

         var tbHistory = _traceBinHistory.ToList();

         tbHistory.Should().HaveCount(67);  // = record count (as created by recordbound transformer)

         var tBin = tbHistory[0];  //ISA
         tBin.Count.Should().Be(1);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);

         tBin = tbHistory[1];  //GS
         tBin.Count.Should().Be(2);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);
         tBin["GS05and08"].Should().BeOfType(typeof(Tuple<int, string>));
         ((Tuple<int, string>)tBin["GS05and08"]).Item1.Should().Be(1719);
         ((Tuple<int, string>)tBin["GS05and08"]).Item2.Should().Be("004010VICS");

         tBin = tbHistory[3];  //BGN
         tBin.Count.Should().Be(2);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);
         tBin["GS05and08"].Should().BeOfType(typeof(Tuple<int, string>));
         ((Tuple<int, string>)tBin["GS05and08"]).Item1.Should().Be(1719);
         ((Tuple<int, string>)tBin["GS05and08"]).Item2.Should().Be("004010VICS");

         tBin = tbHistory[19];  //GE
         tBin.Count.Should().Be(1);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);

         tBin = tbHistory[20];  //IEA
         tBin.Should().BeNull();

         tBin = tbHistory[21];  //ISA
         tBin.Count.Should().Be(1);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);

         tBin = tbHistory[22];  //GS
         tBin.Count.Should().Be(2);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);
         tBin["GS05and08"].Should().BeOfType(typeof(Tuple<int, string>));
         ((Tuple<int, string>)tBin["GS05and08"]).Item1.Should().Be(802);
         ((Tuple<int, string>)tBin["GS05and08"]).Item2.Should().Be("005010X222");

         tBin = tbHistory[23];  //ST
         tBin.Count.Should().Be(2);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);
         tBin["GS05and08"].Should().BeOfType(typeof(Tuple<int, string>));
         ((Tuple<int, string>)tBin["GS05and08"]).Item1.Should().Be(802);
         ((Tuple<int, string>)tBin["GS05and08"]).Item2.Should().Be("005010X222");

         tBin = tbHistory[65];  //GE
         tBin.Count.Should().Be(1);
         tBin["int"].Should().BeOfType(typeof(int));
         tBin["int"].Should().Be(157);

         tBin = tbHistory[66];  //IEA
         tBin.Should().BeNull();
      }


      [Fact]
      public void RecordInitiator_X12TraceBinSetForISAandGS_CorrectContentsInClstrTrfmr()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return new string[] { "ISA", "GS", "ST", "GE", "IEA" }.Contains(rec["Segment"]); };  //each transaction is own cluster (also single envelope marking segments)
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.RecordInitiator = (rec, tb) =>
         {
            switch (rec["Segment"])
            {
               case "ISA": tb.Add("ISA06", (string)rec["Elem006"]); break;
               case "IEA": tb.Clear(); break;
               case "GS": tb.Add("GS08", (string)rec["Elem008"]); break;
               case "GE": tb.Remove("GS08"); break;
            }
            return true;
         };
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = clstr =>
         {
            _traceBinHistory.Enqueue(clstr[clstr.Count - 1].TraceBin);  //use trace bin from last record of each cluster
            return clstr;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(67);  //21 + 46
         results.ClustersRead.Should().Be(10);  // 2 of each: ISA, GS, ST, GE and IEA
         results.RowsWritten.Should().Be(67);
         results.ClustersWritten.Should().Be(10);

         var tbHistory = _traceBinHistory.ToList();

         tbHistory.Should().HaveCount(10);  // = cluster count (as created by clusterbound transformer)

         var tBin = tbHistory[0];  //ISA
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("054318936      ");

         tBin = tbHistory[1];  //GS
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[2];  //SE
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("054318936      ");
         tBin["GS08"].Should().Be("004010VICS");

         tBin = tbHistory[3];  //GE
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("054318936      ");

         tBin = tbHistory[4];  //IEA
         tBin.Should().BeNull();

         tBin = tbHistory[5];  //ISA
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");

         tBin = tbHistory[6];  //GS
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[7];  //SE
         tBin.Count.Should().Be(2);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");
         tBin["GS08"].Should().Be("005010X222");

         tBin = tbHistory[8];  //GE
         tBin.Count.Should().Be(1);
         tBin["ISA06"].Should().Be("SUBMITTERS.ID  ");

         tBin = tbHistory[9];  //IEA
         tBin.Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_X12TraceBinNotSet_AllNulls()
      {
         //arrange
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = rec =>
         {
            _traceBinHistory.Enqueue(rec.TraceBin);
            return rec;
         };
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(67);  //21 + 46
         results.ClustersRead.Should().Be(67);  //clustering is off (inconsistent with X12), but it's not the point of this test
         results.RowsWritten.Should().Be(67);
         results.ClustersWritten.Should().Be(67);

         var tbHistory = _traceBinHistory.ToList();

         tbHistory.Should().HaveCount(67);  // = record count (as created by recordbound transformer)

         tbHistory[0].Should().BeNull();  //ISA
         tbHistory[1].Should().BeNull();  //GS
         tbHistory[2].Should().BeNull();  //ST
         tbHistory[3].Should().BeNull();  //BGN
         tbHistory[18].Should().BeNull();  //SE
         tbHistory[19].Should().BeNull();  //GE
         tbHistory[20].Should().BeNull();  //IEA
         tbHistory[21].Should().BeNull();  //ISA
         tbHistory[22].Should().BeNull();  //GS
         tbHistory[23].Should().BeNull();  //ST
         tbHistory[24].Should().BeNull();  //BHT
         tbHistory[34].Should().BeNull();  //NM1
         tbHistory[64].Should().BeNull();  //SE
         tbHistory[65].Should().BeNull();  //GE
         tbHistory[66].Should().BeNull();  //IEA
      }

   }
}
