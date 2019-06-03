//EtlOrchestrator_tests_X12Output.cs
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
using System.Collections.Generic;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Output
{
   public class EtlOrchestrator_tests_X12Output
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "ISA+00+          +00+          +01+054318936      +01+123456789      +020801+0900+U+00501+00000012 +0+T+~\r";  //note that segement delimiter (\r) is irrelevant here as only comes to play in output writer, whcih is not tested here.
         yield return "GS+PO+4405197800+999999999+20101127+1719+1421+X+004010VICS";
         yield return "ST+834+0001";
         yield return "BGN+00+1234+20001227+0838+PT+++2";
         yield return "N1+P5++FI+954529603";
         yield return "INS+Y+18+021+28+A+E++FT";
         yield return "REF+0F+123456789";
         yield return "REF+1L+G86W553";
         yield return "DTP+356+D8+20001220";
         yield return "NM1+IL+1+STEPHENS+MARIE+V+++34+123456789";
         yield return "PER+IP++HP+4152296748+WP+4152968732";
         yield return "N3+123 MAIN STREET";
         yield return "N4+SAN FRANCISCO+CA+94515";
         yield return "DMG+D8+19691017+F+M";
         yield return "HD+021++HLT+01A3+EMP";
         yield return "DTP+348+D8+20001220";
         yield return "LX+1";
         yield return "NM1+P3+1+FREDRICKSON+STEVE++++XX+1234567891+25";
         yield return "SE+17+0001";
         yield return "GE+1+1421";
         yield return "IEA+1+455321165";
      }

      private IEnumerable<string> _intakeLinesKW()
      {
         //note that these fields make no sense for X12, but it is still a good test
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Susan   \",@pNUM=323";  //note 3 spaces after Susan
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "EOF=GE";
      }

      //Results of the tests are held here:
      private readonly List<string> _resultingLines;

      public EtlOrchestrator_tests_X12Output()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.X12
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;        // to allow fields to showing trailing spaces (should matter for KW, but not X12)
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.X12;
         _config.SetOutputConsumer(l => { if (l != null) _resultingLines.Add(l); });

         _resultingLines = new List<string>();
      }


      [Fact]
      public void ProcessPipeline_X12OutputX12Input_CorrectData()
      {
         //arrange
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(21);
         counts.ClustersWritten.Should().Be(21);
         counts.RowsWritten.Should().Be(21);

         _resultingLines.Should().HaveCount(21);

         //in absence of field delimiter in config, the one from X12 intake (if X12) is used
         _resultingLines[0].Should().Be("ISA+00+          +00+          +01+054318936      +01+123456789      +020801+0900+U+00501+00000012 +0+T+~");
         _resultingLines[1].Should().Be("GS+PO+4405197800+999999999+20101127+1719+1421+X+004010VICS");
         _resultingLines[2].Should().Be("ST+834+0001");
         _resultingLines[3].Should().Be("BGN+00+1234+20001227+0838+PT+++2");
         _resultingLines[4].Should().Be("N1+P5++FI+954529603");
         _resultingLines[11].Should().Be("N3+123 MAIN STREET");
         _resultingLines[15].Should().Be("DTP+348+D8+20001220");
         _resultingLines[16].Should().Be("LX+1");
         _resultingLines[20].Should().Be("IEA+1+455321165");
      }


      [Fact]
      public void ProcessPipeline_X12OutputX12InputCustomFieldDelimiter_CorrectData()
      {
         //arrange
         _config.DefaultX12FieldDelimiter = '^';

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(21);
         counts.ClustersWritten.Should().Be(21);
         counts.RowsWritten.Should().Be(21);

         _resultingLines.Should().HaveCount(21);

         _resultingLines[0].Should().Be("ISA^00^          ^00^          ^01^054318936      ^01^123456789      ^020801^0900^U^00501^00000012 ^0^T^~");
         _resultingLines[1].Should().Be("GS^PO^4405197800^999999999^20101127^1719^1421^X^004010VICS");
         _resultingLines[2].Should().Be("ST^834^0001");
         _resultingLines[3].Should().Be("BGN^00^1234^20001227^0838^PT^^^2");
         _resultingLines[4].Should().Be("N1^P5^^FI^954529603");
         _resultingLines[11].Should().Be("N3^123 MAIN STREET");
         _resultingLines[15].Should().Be("DTP^348^D8^20001220");
         _resultingLines[16].Should().Be("LX^1");
         _resultingLines[20].Should().Be("IEA^1^455321165");
      }


      [Fact]
      public void ProcessPipeline_X12OutputKwInputSegmentAdded_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesKW()).StringSupplier);
         _config.InputKeyPrefix = "@p";
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = c => {
            if ((string)c[0]["RECTYPE"] == "XYZ")  // add NM1 segment after XYZ, i.e. first rec
            {
               c.AddRecord(c[0].CreateEmptyX12Segment("NM1", 4));
               c[c.Count - 1][3] = "Smith";
               c[c.Count - 1]["Elem004"] = "Lucie";
            }
            return c;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(6);  // 5 + 1 (NM1 added to 1st cluster)

         _resultingLines.Count.Should().Be(6);

         _resultingLines[0].Should().Be("XYZ*Mary*123");
         _resultingLines[1].Should().Be("NM1***Smith*Lucie");
         _resultingLines[2].Should().Be("ABCD*XYZ00883*Mary*223");
         _resultingLines[3].Should().Be("ABCD*XYZ00883*Susan   *323");
         _resultingLines[4].Should().Be("ABCD*XYZ00883*Mary*423");
         _resultingLines[5].Should().Be("GE");
      }


      [Fact]
      public void ProcessPipeline_X12OutputKwInputHeadAndFootClusters_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesKW()).StringSupplier);
         // _config.InputKeyPrefix = "@p";  //it doesn't matter in this test as field names are not a part of X12 output
         _config.PrependHeadCluster = true;
         _config.AppendFootCluster = true;
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = c => // no transformations, just add 2 records to head cluster and 1 record to foot cluster
         {
            switch (c.StartRecNo)
            {
               case Constants.HeadClusterRecNo: //head cluster
                  var seg = c.ObtainEmptyRecord().CreateEmptyX12Segment("ISA", 16);
                  seg[8] = "1234567890     ";
                  seg[13] = "000000125";
                  c.AddRecord(seg);
                  c.AddRecord(c.ObtainEmptyRecord().CreateEmptyX12Segment("GS", 8));
                  break;
               case Constants.FootClusterRecNo: //foot cluster
                  seg = c.ObtainEmptyRecord().CreateEmptyX12Segment("IEA", 2);
                  seg[1] = "1";
                  seg[2] = "000000125";
                  c.AddRecord(seg);
                  break;
            }
            return c;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(7);  //1 + 5 + 1
         counts.ClustersWritten.Should().Be(7);  //1 + 5 + 1
         counts.RowsWritten.Should().Be(8);  //2 + 5 + 1

         _resultingLines.Count.Should().Be(8);  //2 + 5 + 1

         _resultingLines[0].Should().Be("ISA*  *          *  *          *  *               *  *1234567890     *      *    * *     *000000125* * * ");  //not a true ISA segment, but OK for test
         _resultingLines[1].Should().Be("GS********");
         _resultingLines[2].Should().Be("XYZ*Mary*123");
         _resultingLines[3].Should().Be("ABCD*XYZ00883*Mary*223");
         _resultingLines[4].Should().Be("ABCD*XYZ00883*Susan   *323");
         _resultingLines[5].Should().Be("ABCD*XYZ00883*Mary*423");
         _resultingLines[6].Should().Be("GE");
         _resultingLines[7].Should().Be("IEA*1*000000125");
      }

   }
}
