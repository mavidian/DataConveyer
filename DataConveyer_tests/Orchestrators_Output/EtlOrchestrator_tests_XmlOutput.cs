//EtlOrchestrator_tests_XmlOutput.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataConveyer_tests.Orchestrators_Output
{
   [TestClass]
   public class EtlOrchestrator_tests_XmlOutput
   {
      OrchestratorConfig _config;

      int _inPtr = 0;
      List<Tuple<ExternalLine, int>> _inLines;

      private Tuple<ExternalLine, int> _inLine(IGlobalCache gc)
      {
         if (_inPtr >= _inLines.Count) return null;
         return _inLines[_inPtr++];
      }

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123,@pDOB=6/5/88";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Susan   \",@pNUM=323";  //note 3 spaces after Susan
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "EOF";
      }

      //Test results:
      TextWriter _outputWriter1, _outputWriter2;
      StringBuilder _xmlOutput1, _xmlOutput2;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         _xmlOutput1 = new StringBuilder();
         _outputWriter1 = new StringWriter(_xmlOutput1);
         _xmlOutput2 = new StringBuilder();
         _outputWriter2 = new StringWriter(_xmlOutput2);

         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();
      }


      [TestMethod]
      public void processXmlOutput_SimpleConfig_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNode|Root/Members,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _xmlOutput1.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members>
    <Member>
      <RECTYPE>XYZ</RECTYPE>
      <NAME>Mary</NAME>
      <NUM>123</NUM>
      <DOB>6/5/88</DOB>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>223</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Susan   </NAME>
      <NUM>323</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>423</NUM>
    </Member>
    <Member>
      <EOF />
    </Member>
  </Members>
</Root>"
            );
      }


      [TestMethod]
      public void processXmlOutput_SimpleConfigWithTypeDefFormat_FormatRespected()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, "00000") : key == "DOB" ? new ItemDef(ItemType.DateTime,"yyyyMMdd") : new ItemDef(ItemType.String, null);  //NUM Int w/format, DOB DateTime w/format everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNode|Root/Members,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _xmlOutput1.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members>
    <Member>
      <RECTYPE>XYZ</RECTYPE>
      <NAME>Mary</NAME>
      <NUM>00123</NUM>
      <DOB>19880605</DOB>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>00223</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Susan   </NAME>
      <NUM>00323</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>00423</NUM>
    </Member>
    <Member>
      <EOF />
    </Member>
  </Members>
</Root>"
            );
      }


      [TestMethod]
      public void processXmlOutput_NoRecordNode_DefaultName()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNODE|Root/Members,RecordNODE|Member,IndentChars|  ";  //notice misspellings (names are case-sensitive); pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         //Missing (bad) CollectionNode resulted in XML fragment (no root)
         //Missing (bad) RecordNode was substituted by default value "__record__"
         _xmlOutput1.ToString().Should().Be(@"<__record__>
  <RECTYPE>XYZ</RECTYPE>
  <NAME>Mary</NAME>
  <NUM>123</NUM>
  <DOB>6/5/88</DOB>
</__record__>
<__record__>
  <RECTYPE>ABCD</RECTYPE>
  <ABCD_ID>XYZ00883</ABCD_ID>
  <NAME>Mary</NAME>
  <NUM>223</NUM>
</__record__>
<__record__>
  <RECTYPE>ABCD</RECTYPE>
  <ABCD_ID>XYZ00883</ABCD_ID>
  <NAME>Susan   </NAME>
  <NUM>323</NUM>
</__record__>
<__record__>
  <RECTYPE>ABCD</RECTYPE>
  <ABCD_ID>XYZ00883</ABCD_ID>
  <NAME>Mary</NAME>
  <NUM>423</NUM>
</__record__>
<__record__>
  <EOF />
</__record__>"
            );
      }


      [TestMethod]
      public void processXmlOutput_AttributeFields_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : key == "DOB" ? new ItemDef(ItemType.DateTime, "yyyyMMdd") : new ItemDef(ItemType.String, null);  //NUM Int, DOB DateTime w/format everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNode|Root/Members,RecordNode|Member,AttributeFields|NUM;RECTYPE;ABCD_ID;DOB,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _xmlOutput1.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members>
    <Member RECTYPE=""XYZ"" NUM=""123"" DOB=""19880605"">
      <NAME>Mary</NAME>
    </Member>
    <Member RECTYPE=""ABCD"" ABCD_ID=""XYZ00883"" NUM=""223"">
      <NAME>Mary</NAME>
    </Member>
    <Member RECTYPE=""ABCD"" ABCD_ID=""XYZ00883"" NUM=""323"">
      <NAME>Susan   </NAME>
    </Member>
    <Member RECTYPE=""ABCD"" ABCD_ID=""XYZ00883"" NUM=""423"">
      <NAME>Mary</NAME>
    </Member>
    <Member>
      <EOF />
    </Member>
  </Members>
</Root>"
            );
      }


      [TestMethod]
      public void processXmlOutput_ObserveClusters_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 2 == 0; };  //two record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.RecordFilter;
         _config.RecordFilterPredicate = r => !r.ContainsKey("EOF");  // filter out last record
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNode|Root,ClusterNode|Family,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(3);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(4);

         _xmlOutput1.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Family>
    <Member>
      <RECTYPE>XYZ</RECTYPE>
      <NAME>Mary</NAME>
      <NUM>123</NUM>
      <DOB>6/5/88</DOB>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>223</NUM>
    </Member>
  </Family>
  <Family>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Susan   </NAME>
      <NUM>323</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>423</NUM>
    </Member>
  </Family>
</Root>"
            );
      }


      [TestMethod]
      public void processXmlOutput_TwoTargets_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => c.ClstrNo % 2 + 1;  //target1 - even clusters; target2 - odd clusters
         _config.OutputDataKind = KindOfTextData.XML;
         _config.XmlJsonOutputSettings = "CollectionNode|Root/Members,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriters = () => new[] { _outputWriter1, _outputWriter2 }; ;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         //1st target (even clusters):
         _xmlOutput1.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>223</NUM>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Mary</NAME>
      <NUM>423</NUM>
    </Member>
  </Members>
</Root>"
            );

         //2nd target (odd clusters):
         _xmlOutput2.ToString().Should().Be(@"<?xml version=""1.0"" encoding=""utf-16""?>
<Root>
  <Members>
    <Member>
      <RECTYPE>XYZ</RECTYPE>
      <NAME>Mary</NAME>
      <NUM>123</NUM>
      <DOB>6/5/88</DOB>
    </Member>
    <Member>
      <RECTYPE>ABCD</RECTYPE>
      <ABCD_ID>XYZ00883</ABCD_ID>
      <NAME>Susan   </NAME>
      <NUM>323</NUM>
    </Member>
    <Member>
      <EOF />
    </Member>
  </Members>
</Root>"
            );
      }

   }
}
