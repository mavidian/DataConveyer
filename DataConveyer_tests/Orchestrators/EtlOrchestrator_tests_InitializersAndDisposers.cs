//EtlOrchestrator_tests_InitializersAndDisposers.cs
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


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataConveyer_tests.Orchestrators
{

   [TestClass]
   public class EtlOrchestrator_tests_InitializersAndDisposers
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
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "EOF";
      }


      //Result of the tests are held here:
      List<string> _resultingLines;  //container of the test results

      int _initDispResult;


      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         //prepare extraction of the results from the pipeline
         _resultingLines = new List<string>();
         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();

         _initDispResult = 0;
      }


      [TestMethod]
      public void processPipeline_NoInitsOrDispsConfigured_NoInitOrDispsCalled()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = clstr => { return true; };
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(5);
         result.RowsWritten.Should().Be(5);

         _resultingLines.Count.Should().Be(6);  //5 from intake + null EOF

         _initDispResult.Should().Be(0);
      }


      [TestMethod]
      public void processPipeline_OnlyDispOnOutputConfigured_DispOnOutputCalled()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = clstr => { return true; };
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDisposer = gc => _initDispResult += 17;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(5);
         result.RowsWritten.Should().Be(5);

         _resultingLines.Count.Should().Be(6);  //5 from intake + null EOF

         _initDispResult.Should().Be(17);
      }


      [TestMethod]
      public void processPipeline_All4InitsAndDispsConfigured_All4InitsAndDispsCalled()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.IntakeInitializer = gc => { _initDispResult++; return null; };
         _config.IntakeDisposer = gc => _initDispResult += _initDispResult * 3;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = clstr => { return true; };
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputInitializer = gc => { _initDispResult += _initDispResult * 2; return null; };
         _config.OutputDisposer = gc => _initDispResult += _initDispResult * 4; ;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(5);
         result.RowsWritten.Should().Be(5);

         _resultingLines.Count.Should().Be(6);  //5 from intake + null EOF

         _initDispResult.Should().Be(60);  // 1 + 1*2 + (1 + 1*2)*3 + (1 + 1*2 + (1 + 1*2)*3)*4 = 1+2+9+48 = 60   
      }


      [TestMethod]
      public void processPipeline_BothInitsConfiguredButIntakeThrows_OutputInitNotCalled()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.IntakeInitializer = gc => { _initDispResult++; throw new NotSupportedException(); };
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = clstr => { return true; };
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputInitializer = gc => { _initDispResult += _initDispResult * 2; return null; };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         _resultingLines.Count.Should().Be(0);

         _initDispResult.Should().Be(1);  //only ++ from intake init
      }


      [TestMethod]
      public void processPipeline_BothInitsConfiguredIntakeThrowsEagerInit_OutputInitCalled()
      {
         //arrange
         _config.EagerInitialization = true;
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.IntakeInitializer = gc => { _initDispResult++; throw new NotSupportedException(); };
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = clstr => { return true; };
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputInitializer = gc => { _initDispResult += _initDispResult * 2; return null; };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         _resultingLines.Count.Should().Be(0);

         _initDispResult.Should().Be(3);  // 1 + 1*2  
      }
   }
}
