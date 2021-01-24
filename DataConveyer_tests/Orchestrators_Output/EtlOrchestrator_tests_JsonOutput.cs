//EtlOrchestrator_tests_JsonOutput.cs
//
// Copyright © 2018-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   public class EtlOrchestrator_tests_JsonOutput
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
      StringBuilder _jsonOutput1, _jsonOutput2;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         _jsonOutput1 = new StringBuilder();
         _outputWriter1 = new StringWriter(_jsonOutput1);
         _jsonOutput2 = new StringBuilder();
         _outputWriter2 = new StringWriter(_jsonOutput2);

         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();
      }


      [TestMethod]
      public void processJsonOutput_SimpleConfig_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         //note that this test does not have types defined, hence all fields (incl. NUM) are of String type
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
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

         _jsonOutput1.ToString().Should().Be(@"{
  ""Root"": {
    ""Members"": {
      ""Member"": [
        {
          ""RECTYPE"": ""XYZ"",
          ""NAME"": ""Mary"",
          ""NUM"": ""123"",
          ""DOB"": ""6/5/88""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""223""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Susan   "",
          ""NUM"": ""323""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""423""
        },
        {
          ""EOF"": null
        }
      ]
    }
  }
}"
            );
      }


      [TestMethod]
      public void processJsonOutputAsync_SimpleConfig_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         //note that this test does not have types defined, hence all fields (incl. NUM) are of String type
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = "CollectionNode|Root/Members,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;
         _config.AsyncOutput = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _jsonOutput1.ToString().Should().Be(@"{
  ""Root"": {
    ""Members"": {
      ""Member"": [
        {
          ""RECTYPE"": ""XYZ"",
          ""NAME"": ""Mary"",
          ""NUM"": ""123"",
          ""DOB"": ""6/5/88""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""223""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Susan   "",
          ""NUM"": ""323""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""423""
        },
        {
          ""EOF"": null
        }
      ]
    }
  }
}"
            );
      }


      [TestMethod]
      public void processJsonOutput_NoNodesDefinedAtAll_MultipleJsonContents()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         //note that this test does not have types defined, hence all fields (incl. NUM) are of String type
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = "CollectionNODE|Root/Members,RecordNODE|Member,IndentChars|  ";  //!!!notice misspellings (names are case-sensitive); pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         //This is a special case where all 3 nodes are missing or bad.
         //It results in multiple JSON objects (technically not a valid JSON).
         _jsonOutput1.ToString().Should().Be(@"{
  ""RECTYPE"": ""XYZ"",
  ""NAME"": ""Mary"",
  ""NUM"": ""123"",
  ""DOB"": ""6/5/88""
}
{
  ""RECTYPE"": ""ABCD"",
  ""ABCD_ID"": ""XYZ00883"",
  ""NAME"": ""Mary"",
  ""NUM"": ""223""
}
{
  ""RECTYPE"": ""ABCD"",
  ""ABCD_ID"": ""XYZ00883"",
  ""NAME"": ""Susan   "",
  ""NUM"": ""323""
}
{
  ""RECTYPE"": ""ABCD"",
  ""ABCD_ID"": ""XYZ00883"",
  ""NAME"": ""Mary"",
  ""NUM"": ""423""
}
{
  ""EOF"": null
}"
            );
      }


      [TestMethod]
      public void processJsonOutput_EmptyRecordNode_ArrayNotObject()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = "RecordNode|,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         //Missing CollectionNode combined with empty RecordNode results in JSON containing top level array and not object.
         //RecordNode is mandatory except for a special case where all 3 nodes are absent.
         _jsonOutput1.ToString().Should().Be(@"[
  {
    ""RECTYPE"": ""XYZ"",
    ""NAME"": ""Mary"",
    ""NUM"": ""123"",
    ""DOB"": ""6/5/88""
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Mary"",
    ""NUM"": ""223""
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Susan   "",
    ""NUM"": ""323""
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Mary"",
    ""NUM"": ""423""
  },
  {
    ""EOF"": null
  }
]"
            );
      }


      [TestMethod]
      public void processJsonOutput_TypedItems_FormatIgnoredExceptDate()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ExplicitTypeDefinitions = "NUM|I|00000,DOB|D|yyyyMMdd";
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = "RecordNode|,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         //The same output layout as processJsonOutput_EmptyRecordNode_ArrayNotObject, but the point here is in typed items (defined in ExplicitTypeDefinitions)
         //Note that NUM is written as number (no quotes) with format ignored, but DOB is written as string with format respected.
         _jsonOutput1.ToString().Should().Be(@"[
  {
    ""RECTYPE"": ""XYZ"",
    ""NAME"": ""Mary"",
    ""NUM"": 123,
    ""DOB"": ""19880605""
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Mary"",
    ""NUM"": 223
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Susan   "",
    ""NUM"": 323
  },
  {
    ""RECTYPE"": ""ABCD"",
    ""ABCD_ID"": ""XYZ00883"",
    ""NAME"": ""Mary"",
    ""NUM"": 423
  },
  {
    ""EOF"": null
  }
]"
            );
      }


      [TestMethod]
      public void processJsonOutput_ObserveClusters_CorrectData()
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
         _config.OutputDataKind = KindOfTextData.JSON;
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

         _jsonOutput1.ToString().Should().Be(@"{
  ""Root"": {
    ""Family"": [
      {
        ""Member"": [
          {
            ""RECTYPE"": ""XYZ"",
            ""NAME"": ""Mary"",
            ""NUM"": 123,
            ""DOB"": ""6/5/88""
          },
          {
            ""RECTYPE"": ""ABCD"",
            ""ABCD_ID"": ""XYZ00883"",
            ""NAME"": ""Mary"",
            ""NUM"": 223
          }
        ]
      },
      {
        ""Member"": [
          {
            ""RECTYPE"": ""ABCD"",
            ""ABCD_ID"": ""XYZ00883"",
            ""NAME"": ""Susan   "",
            ""NUM"": 323
          },
          {
            ""RECTYPE"": ""ABCD"",
            ""ABCD_ID"": ""XYZ00883"",
            ""NAME"": ""Mary"",
            ""NUM"": 423
          }
        ]
      }
    ]
  }
}"
            );
      }


      [TestMethod]
      public void processJsonOutput_ObserveClustersWithNoName_CorrectData()
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
         _config.OutputDataKind = KindOfTextData.JSON;
         _config.XmlJsonOutputSettings = "CollectionNode|Root,ClusterNode|,RecordNode|Member,IndentChars|  ";  //pretty-print
         _config.OutputWriter = () => _outputWriter1;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(3);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(4);

         _jsonOutput1.ToString().Should().Be(@"{
  ""Root"": [
    {
      ""Member"": [
        {
          ""RECTYPE"": ""XYZ"",
          ""NAME"": ""Mary"",
          ""NUM"": 123,
          ""DOB"": ""6/5/88""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": 223
        }
      ]
    },
    {
      ""Member"": [
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Susan   "",
          ""NUM"": 323
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": 423
        }
      ]
    }
  ]
}"
            );
      }


      [TestMethod]
      public void processJsonOutput_TwoTargets_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         //note that this test does not have TypeDefiner defined, hence all fields (incl. NUM) are of String type
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => c.ClstrNo % 2 + 1;  //target1 - even clusters; target2 - odd clusters
         _config.OutputDataKind = KindOfTextData.JSON;
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
         _jsonOutput1.ToString().Should().Be(@"{
  ""Root"": {
    ""Members"": {
      ""Member"": [
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""223""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Mary"",
          ""NUM"": ""423""
        }
      ]
    }
  }
}"
            );

         //2nd target (odd clusters):
         _jsonOutput2.ToString().Should().Be(@"{
  ""Root"": {
    ""Members"": {
      ""Member"": [
        {
          ""RECTYPE"": ""XYZ"",
          ""NAME"": ""Mary"",
          ""NUM"": ""123"",
          ""DOB"": ""6/5/88""
        },
        {
          ""RECTYPE"": ""ABCD"",
          ""ABCD_ID"": ""XYZ00883"",
          ""NAME"": ""Susan   "",
          ""NUM"": ""323""
        },
        {
          ""EOF"": null
        }
      ]
    }
  }
}"
            );
      }

   }
}
