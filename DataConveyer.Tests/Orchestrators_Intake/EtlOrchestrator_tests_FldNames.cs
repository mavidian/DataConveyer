//EtlOrchestrator_tests_FldNames.cs
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
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_FldNames
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _csvIntakeLines()
      {
         //Note default field name is Fldxxx (e.g. Fld001)
         yield return "Fld001,Col002,,Fld007,Fld006,,Fld009";
         yield return "Data01,Data02,Data03,Data04,Data05,Data06,Data07,Data08,Data09,Data10,Data11,Data12";
      }

      private IEnumerable<string> _flatIntakeLines()
      {
         //Field widths are: 6,10,8,10
         yield return "Col001Col002    Col003  Col004";
         yield return "Data01Data02    Data03  Data04    ";
      }

      IEnumerable<string> _conflictingCsvIntakeLines()
      {
         //Note default field name is Fldxxx (e.g. Fld001)
         yield return "Fld001,Col002,FldDup,Fld007,Fld006,FldDup,Fld009";  //FldDup is the conflict in the header row
         yield return "Data01,Data02,Data03,Data04,Data05,Data06,Data07,Data08,Data09,Data10,Data11,Data12";
      }

      private readonly ActionBlock<KeyValCluster> _resultsDiscarder;  //block to intercept output from the clustering block

      public EtlOrchestrator_tests_FldNames()
      {
         _config = new OrchestratorConfig
         {
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _resultsDiscarder = new ActionBlock<KeyValCluster>(c => { }); // any output produced by the clustering block is irrelevant for these tests (and hence discarded)

      }


      [Fact]
      public void ProcessPipelineCsvIntake_TrickyHeaderRow_CorrectFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld007");
         fldNames[4].Should().Be("Fld006");
         fldNames[5].Should().Be("Fld008");
         fldNames[6].Should().Be("Fld009");
         fldNames[7].Should().Be("Fld010");
         fldNames[8].Should().Be("Fld011");
         fldNames[9].Should().Be("Fld012");
         fldNames[10].Should().Be("Fld013");
         fldNames[11].Should().Be("Fld014");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_NoConfigNoHeaderRow_DefaultFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Fld002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld004");
         fldNames[4].Should().Be("Fld005");
         fldNames[5].Should().Be("Fld006");
         fldNames[6].Should().Be("Fld007");
         fldNames[7].Should().Be("Fld008");
         fldNames[8].Should().Be("Fld009");
         fldNames[9].Should().Be("Fld010");
         fldNames[10].Should().Be("Fld011");
         fldNames[11].Should().Be("Fld012");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldNamesInConfig_CorrectFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.InputFields = "Fld001,Col002,,Fld007,Fld006,,Fld009";  //the same "tricky" row as in 1st line (which is now interpreted as data line)
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld007");
         fldNames[4].Should().Be("Fld006");
         fldNames[5].Should().Be("Fld008");
         fldNames[6].Should().Be("Fld009");
         fldNames[7].Should().Be("Fld010");
         fldNames[8].Should().Be("Fld011");
         fldNames[9].Should().Be("Fld012");
         fldNames[10].Should().Be("Fld013");
         fldNames[11].Should().Be("Fld014");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_ColNamesInBothConfigAnd1stRow_ConfigWins()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.InputFields = "Fld001,Col002,Col003,Col004";  //this overrules the 1st row data ("Fld001,Col002,,Fld007,Fld006,,Fld009")
                                                               //no type definitions & other defaults
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Col003");
         fldNames[3].Should().Be("Col004");
         fldNames[4].Should().Be("Fld005");
         fldNames[5].Should().Be("Fld006");
         fldNames[6].Should().Be("Fld007");
         fldNames[7].Should().Be("Fld008");
         fldNames[8].Should().Be("Fld009");
         fldNames[9].Should().Be("Fld010");
         fldNames[10].Should().Be("Fld011");
         fldNames[11].Should().Be("Fld012");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldsInConfigButNoNames_FldNamesAsIfNoConfig()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.InputFields = "|10,|10,,|10,|10";  //none of the InputFields has name specified (only widths)                                                  
         _config.AllowOnTheFlyInputFields = true;  //All fields should have default names assigned

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Fld002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld004");
         fldNames[4].Should().Be("Fld005");
         fldNames[5].Should().Be("Fld006");
         fldNames[6].Should().Be("Fld007");
         fldNames[7].Should().Be("Fld008");
         fldNames[8].Should().Be("Fld009");
         fldNames[9].Should().Be("Fld010");
         fldNames[10].Should().Be("Fld011");
         fldNames[11].Should().Be("Fld012");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldsInConfigWithOneName_CorrectFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.InputFields = "|10,Col002|10,,|10,|10";  //2nd field has name specified                                                          
         _config.AllowOnTheFlyInputFields = true; //The remaining fields (i.e. other than the 2nd one) should have default names assigned

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld004");
         fldNames[4].Should().Be("Fld005");
         fldNames[5].Should().Be("Fld006");
         fldNames[6].Should().Be("Fld007");
         fldNames[7].Should().Be("Fld008");
         fldNames[8].Should().Be("Fld009");
         fldNames[9].Should().Be("Fld010");
         fldNames[10].Should().Be("Fld011");
         fldNames[11].Should().Be("Fld012");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_NoConfigNoHeaderRowNoOnTheFly_NoFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.AllowOnTheFlyInputFields = false;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(0);
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldsInConfigButNoNamesNoOnTheFly_NoFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.InputFields = "|10,|10,,|10,|10";  //none of the InputFields has name specified (only widths) - in case of Delimited data this is treated as no field names                                                

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(0);
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldsInConfigWithOneNameNoOnTheFly_CorrectFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;  //1st row interpreted as data line
         _config.InputFields = "|10,Col002|10,,|10,|10";  //2nd field has name specified, so all 5 fields will have names assigned (default names for the remaining 4)
         _config.AllowOnTheFlyInputFields = false;  //no fields are allowed to be addedd

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(5);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld004");
         fldNames[4].Should().Be("Fld005");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_FieldsInConfigButNoNames1stRowHeader_HeaderRowWins()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;  // in this case, column names from header row will be used as none of the InputFields have name
         _config.InputFields = "|10,|10,,|10,|10";  //none of the InputFields has name specified (only widths) - in case of Delimited data this is treated as no field names                                              
         _config.AllowOnTheFlyInputFields = false;  //no fields are allowed to be added

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(7);  //note that header row has 7 fields in it
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld007");
         fldNames[4].Should().Be("Fld006");
         fldNames[5].Should().Be("Fld008");
         fldNames[6].Should().Be("Fld009");
      }


      [Fact]
      public void ProcessPipelineFlatIntake_NoFldNamesInConfigButHeaderRow_FldNamesFromHeaderRow()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Flat;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_flatIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.TrimInputValues = true;
         _config.InputFields = "|6,|10,|8,|10";  //none of the InputFields has name specified (only widths)                                             
                                                 //no type definitions & other defaults

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(4);
         fldNames[0].Should().Be("Col001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Col003");
         fldNames[3].Should().Be("Col004");
      }


      [Fact]
      public void ProcessPipelineFlatIntake_NoFldNamesInConfigNoHeader_DefaultFldNames()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Flat;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_flatIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.InputFields = "|6,|10,|8,|10";  //none of the InputFields has name specified (only widths)                                             
                                                 //no type definitions & other defaults

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(4);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Fld002");
         fldNames[2].Should().Be("Fld003");
         fldNames[3].Should().Be("Fld004");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_ConflictingFldNames_DefaultSubstituted()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.InputFields = "Fld001,Col002,Col003,Col004,BadCol,Col006,BadCol";  //this overrules the 1st row data ("Fld001,Col002,,Fld007,Fld006,,Fld009")
                                                                                    //note repeated column name of BadCol
                                                                                    //no type definitions & other defaults
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Col003");
         fldNames[3].Should().Be("Col004");
         fldNames[4].Should().Be("BadCol");
         fldNames[5].Should().Be("Col006");
         fldNames[6].Should().Be("Fld007");  //due to conflict (dup name) BadCol was substituted by default name
         fldNames[7].Should().Be("Fld008");
         fldNames[8].Should().Be("Fld009");
         fldNames[9].Should().Be("Fld010");
         fldNames[10].Should().Be("Fld011");
         fldNames[11].Should().Be("Fld012");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_ConflictingFldNamesNoOTFIF_DefaultSubstituted()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_csvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.InputFields = "Fld001,Col002,Col003,Col004,BadCol,Col006,BadCol";  //this overrules the 1st row data ("Fld001,Col002,,Fld007,Fld006,,Fld009")
                                                                                    //note repeated column name of BadCol
                                                                                    //no type definitions & other defaults

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(7);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("Col003");
         fldNames[3].Should().Be("Col004");
         fldNames[4].Should().Be("BadCol");
         fldNames[5].Should().Be("Col006");
         fldNames[6].Should().Be("Fld007");  //due to conflict (dup name) BadCol was substituted by default name
      }


      [Fact]
      public void ProcessPipelineCsvIntake_ConflictingFldNamesFromHeader_DefaultSubstituted()
      {
         //arrange
         //here, conflicting header is provided via 1st row (i.e. header row)
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_conflictingCsvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(7);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("FldDup");
         fldNames[3].Should().Be("Fld007");
         fldNames[4].Should().Be("Fld006");
         fldNames[5].Should().Be("Fld008");  //next available default name
         fldNames[6].Should().Be("Fld009");
      }


      [Fact]
      public void ProcessPipelineCsvIntake_ConflictingFldNamesFromHeaderOTFIFallowed_DefaultSubstituted()
      {
         //arrange
         //here, conflicting header is provided via 1st row (i.e. header row)
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_conflictingCsvIntakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsDiscarder);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsDiscarder.Completion.Wait();

         //assert
         var fldNames = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;
         fldNames.Count.Should().Be(12);
         fldNames[0].Should().Be("Fld001");
         fldNames[1].Should().Be("Col002");
         fldNames[2].Should().Be("FldDup");
         fldNames[3].Should().Be("Fld007");
         fldNames[4].Should().Be("Fld006");
         fldNames[5].Should().Be("Fld008");  //next available default name
         fldNames[6].Should().Be("Fld009");
         fldNames[7].Should().Be("Fld010");
         fldNames[8].Should().Be("Fld011");
         fldNames[9].Should().Be("Fld012");
         fldNames[10].Should().Be("Fld013");
         fldNames[11].Should().Be("Fld014");
      }
   }
}
