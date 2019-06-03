//EtlOrchestrator_tests_failures.cs
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


using DataConveyer_tests.Logging;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;


namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class EtlOrchestrator_tests_failures
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
         yield return "FIRST_NAME,LAST_NAME,DOB";
         yield return "Loren,Asar,1/3/1966";
         yield return "Lara,Gudroe,5/19/1974";
         yield return "Shawna,Palaspas,6/1/1960";
         yield return "Erick,Nievas,4/15/1950";
         yield return "Jess,Chaffins,7/12/1969";
      }

      //Result of the tests are held here:
      List<KeyValCluster> _resultingClusters;  //container of the test results
      ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig(new MockLogger(LogEntrySeverity.Warning));

         //prepare extraction of the results from the pipeline
         _resultingClusters = new List<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Add(c));
         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();
      }


      [TestMethod]
      public void processPipeline_Baseline_NoErrorsAndDataCorrect()
      {
         //This is a baseline test, which does not throw exceptions

         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
            return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPO = new PrivateObject(orchestrator);
         var transformingBlock = (TransformManyBlock<KeyValCluster, KeyValCluster>)orchestratorPO.GetField("_transformingBlock");
         transformingBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var results = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(6);
         results.ClustersRead.Should().Be(5);
         results.RowsWritten.Should().Be(0);  //no output invoked during the test
         results.ClustersWritten.Should().Be(0);

         _resultingClusters.Count.Should().Be(5);
         _resultingClusters[0].Count.Should().Be(1);
         _resultingClusters[1].Count.Should().Be(1);
         _resultingClusters[4].Count.Should().Be(1);

         IRecord outRec = _resultingClusters[0][0];
         IItem age = outRec.GetItem("AGE");
         age.Value.Should().Be(50);
         IItem dob = outRec.GetItem("DOB");
         dob.Value.Should().Be(new DateTime(1966, 1, 3));
         dob.StringValue.Should().Be("1/3/1966");
      }


      [TestMethod]
      public void processPipeline_TransformThrows_TransformFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            throw new InvalidOperationException("My Bad");
         };
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         results.RowsRead.Should().Be(6);       //we shouldn't be verifying this (as transformer could've thrown before all records are read), it just happens to work this way
         results.ClustersRead.Should().Be(5);
         results.RowsWritten.Should().Be(0);    //no output invoked during the test
         results.ClustersWritten.Should().Be(0);

         _resultingClusters.Count.Should().Be(0);  //this is obvious condition as we don't even have _resultsExtractor connected

         errorDetails.Item1.Should().Be("transforming block");
         errorDetails.Item2.Should().Be(" at cluster #1");
         errorDetails.Item3.Should().BeOfType(typeof(InvalidOperationException));
         errorDetails.Item3.Message.Should().Be("My Bad");
      }

      [TestMethod]
      public void processPipeline_RouterThrows_RouterFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => { throw new MissingFieldException("Bad route"); };
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         //results.RowsRead.Should().Be(6);       //we shouldn't be verifying this (as transformer could've thrown before all records are read), it just happens to work this way
         //results.ClustersRead.Should().Be(5);
         results.RowsWritten.Should().Be(0);    //no output invoked during the test
         results.ClustersWritten.Should().Be(1);  // the first cluster processing was complete before routing exception occurred

         _resultingClusters.Count.Should().Be(0);  //this is obvious condition as we don't even have _resultsExtractor connected

         errorDetails.Item1.Should().Be("unclustering block");
         errorDetails.Item2.Should().Be(" at cluster #1");
         errorDetails.Item3.Should().BeOfType(typeof(MissingFieldException));
         errorDetails.Item3.Message.Should().Be("Bad route");
      }


      [TestMethod]
      public void processPipeline_OutputThrows_OutputFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
               return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) =>
         {
            throw new DivideByZeroException("bad math");
         };
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         results.RowsRead.Should().Be(6);       //we shouldn't be verifying this (as transformer could've thrown before all records are read), it just happens to work this way
         results.ClustersRead.Should().Be(5);

         _resultingClusters.Count.Should().Be(0);  //this is obvious condition as we don't even have _resultsExtractor connected

         errorDetails.Item1.Should().Be("output block");
         errorDetails.Item2.Should().Be(" at line starting with 'Loren Asar1/3/1'");
         errorDetails.Item3.Should().BeOfType(typeof(DivideByZeroException));
         errorDetails.Item3.Message.Should().Be("bad math");
      }

      [TestMethod]
      public void processPipeline_AsyncOutputThrows_OutputFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
               return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.AsyncOutput = true;
         _config.AsyncOutputConsumer = (t, gc) => { throw new IndexOutOfRangeException("bad index"); };
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         results.RowsRead.Should().Be(6);       //we shouldn't be verifying this (as transformer could've thrown before all records are read), it just happens to work this way
         results.ClustersRead.Should().Be(5);

         _resultingClusters.Count.Should().Be(0);  //this is obvious condition as we don't even have _resultsExtractor connected

         errorDetails.Item1.Should().Be("output block");
         errorDetails.Item2.Should().Be(" at line starting with 'Loren Asar1/3/1'");
         errorDetails.Item3.Should().BeOfType(typeof(IndexOutOfRangeException));
         errorDetails.Item3.Message.Should().Be("bad index");
      }


      [TestMethod]
      public void processPipeline_IntakeThrows_IntakeFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = gc => { throw new DataMisalignedException("misaligned"); };
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
               return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         results.RowsRead.Should().Be(0);
         results.ClustersRead.Should().Be(0);

         errorDetails.Item1.Should().Be("header intake");
         errorDetails.Item2.Should().Be(" after line #0");   //intake supplier throws on the very first call, i.e. didn't even supply line #1
         errorDetails.Item3.Should().BeOfType(typeof(DataMisalignedException));
         errorDetails.Item3.Message.Should().Be("misaligned");
      }


      [TestMethod]
      public void processPipeline_AsyncIntakeThrows_IntakeFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.AsyncIntake = true;
         _config.AsyncIntakeSupplier = gc => { throw new FormatException("bad format"); };
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
               return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);
         results.RowsRead.Should().Be(0);
         results.ClustersRead.Should().Be(0);

         errorDetails.Item1.Should().Be("header intake");
         errorDetails.Item2.Should().Be(" after line #0");   //intake supplier throws on the very first call, i.e. didn't even supply line #1
         errorDetails.Item3.Should().BeOfType(typeof(FormatException));
         errorDetails.Item3.Message.Should().Be("bad format");
      }


      [TestMethod]
      public void processPipeline_TypeDefinerThrows_LineParsingFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.TypeDefiner = fn => { throw new MissingFieldException("missing field"); };
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
            return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);

         errorDetails.Item1.Should().Be("line parsing block");
         errorDetails.Item2.Should().Be(" at line #2"); //note that line #1 is a header row
         errorDetails.Item3.Should().BeOfType(typeof(MissingFieldException));
         errorDetails.Item3.Message.Should().Be("missing field");
      }


      [TestMethod]
      public void RecordInitiator_RecordInitiatorThrows_TraceBinSettingFault()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.RecordInitiator = (rec, tb) => { if (rec.RecNo == 3) throw new MissingMemberException("Missing Member"); return true; };
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);

         errorDetails.Item1.Should().Be("initiating record");
         errorDetails.Item2.Should().Be(" at line #4"); //note that line #1 is a header row (and the exception is thrown on 3rd data row)
         errorDetails.Item3.Should().BeOfType(typeof(MissingMemberException));
         errorDetails.Item3.Message.Should().Be("Missing Member");
      }


      [TestMethod]
      public void processPipeline_ClusterMarkerThrows_ClusteringFault()
      {
         //arrange
         _config.InputDataKind           = KindOfTextData.Delimited;
         _config.IntakeSupplier          = _inLine;
         _config.ExplicitTypeDefinitions = "AGE|I,DOB|D|M/d/yyyy";
         _config.HeadersInFirstInputRow  = true;
         _config.RetainQuotes            = true;
         _config.ClusterMarker           = (rec, prevRec, recCnt) => { throw new NotSupportedException("not supported"); };
         _config.TransformerType         = TransformerType.Recordbound;
         _config.RecordboundTransformer  = r =>
         {
            var asOf = new DateTime(2017, 1, 1);
            dynamic rO = r.GetEmptyClone();
            dynamic rI = r;
            rO.NAME = rI.FIRST_NAME + " " + rI.LAST_NAME;
            rO.DOB = rI.DOB;
            rO.AGE = (int)((asOf - rI.DOB).TotalDays / 365.2422);  //approximation
               return rO;
         };
         _config.AllowTransformToAlterFields = true; // otherwise, no new fields would've been allowed
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer
         Tuple<string, string, Exception> errorDetails = null;  //Item1=origin, Item2=context, Item3=exception
         _config.ErrorOccurredHandler = (s, e) => { errorDetails = Tuple.Create(e.Origin, e.Context, e.Exception); };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var results = orchestrator.ExecuteAsync().Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.Failed);

         errorDetails.Item1.Should().Be("clustering block");
         errorDetails.Item2.Should().Be(" at record #1");  //first record
         errorDetails.Item3.Should().BeOfType(typeof(NotSupportedException));
         errorDetails.Item3.Message.Should().Be("not supported");
      }


      [TestMethod]
      public void processPipeline_DisposeCalledWhileRunning_IgnoreButLogWarning()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            Thread.Sleep(5);
            return r;
         };
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPO = new PrivateObject(orchestrator);
         var transformingBlock = (TransformManyBlock<KeyValCluster, KeyValCluster>)orchestratorPO.GetField("_transformingBlock");
         transformingBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         var logger = (MockLogger)_config.Logger;

         //act
         var executionTask = orchestrator.ExecuteAsync();
         Thread.Sleep(10);  //long enough to launch processing (although it is not really needed for this test)

         orchestrator.Dispose();
         var results = executionTask.Result;

         //assert
         results.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results.RowsRead.Should().Be(6);
         results.ClustersRead.Should().Be(5);
         results.RowsWritten.Should().Be(0);  //no output invoked during the test
         results.ClustersWritten.Should().Be(0);

         _resultingClusters.Count.Should().Be(5);
         _resultingClusters[0].Count.Should().Be(1);
         _resultingClusters[1].Count.Should().Be(1);
         _resultingClusters[4].Count.Should().Be(1);

         logger.Results.Count.Should().Be(2);  //first one is log title box
         //Tuple contained in MockLogger.Results: Item1=severity, Item2=message, Item3=entry sent to logger
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logEntry = logger.Results[1];
         logEntry.Item1.Should().Be(LogEntrySeverity.Warning);
         logEntry.Item2.Contains("Dispose called while processing, ").Should().BeTrue();

         //act2
         orchestrator.Dispose();  //this call is after completion and should not trigger the warning
                                  //however, since CloseLoggerOnDispose is true (default), it adds the logger closed message

         //assert2
         logger.Results.Count.Should().Be(3);
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);     //log title box
         logger.Results[1].Item1.Should().Be(LogEntrySeverity.Warning);  //"Dispose called while processing" that existed before act2
         logger.Results[2].Item1.Should().Be(LogEntrySeverity.None);     //log close message

      }


      [TestMethod]
      public void processPipeline_DisposeCalledBeforeStart_PreventsSubsequeuntExecution()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPO = new PrivateObject(orchestrator);
         var transformingBlock = (TransformManyBlock<KeyValCluster, KeyValCluster>)orchestratorPO.GetField("_transformingBlock");
         transformingBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         var logger = (MockLogger)_config.Logger;

         //act
         orchestrator.Dispose();

         //assert
         _resultingClusters.Count.Should().Be(0);

         //act2
         var executionTask = orchestrator.ExecuteAsync();  //ExecuteAsync after Dispose
         var results = executionTask.Result;

         //assert2
         results.CompletionStatus.Should().Be(CompletionStatus.InvalidAttempt);
         results.RowsRead.Should().Be(0);
         results.ClustersRead.Should().Be(0);
         results.RowsWritten.Should().Be(0);
         results.ClustersWritten.Should().Be(0);

         logger.Results.Count.Should().Be(3);
         //Tuple contained in MockLogger.Results: Item1=severity, Item2=message, Item3=entry sent to logger
         //Note that this is a special case where the logger would've been closed before the 
         // however, the MockLogger ignores the Dispose method call and continues adding messages to the Results array
         //As a result, we have a situation specific to the MockLogger where the Fatal error ("Invalid attempt..."
         // is reported AFTER the Log close message(!)
         //In a "real" logger (e.g. LogFile), an exception (e.g.ObjectDisposedException: Cannot write to a closed TextWriter)
         // would've been thrown; prompting CloseLoggerOnDispose = false setting to diagnose the scenario further like
         // illustrated in the next test below.
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         logger.Results[1].Item1.Should().Be(LogEntrySeverity.None);  //log close message
         var logEntry = logger.Results[2];
         logEntry.Item1.Should().Be(LogEntrySeverity.Fatal);
         logEntry.Item2.Contains("Invalid attempt to start execution ").Should().BeTrue();
      }


      [TestMethod]
      public void processPipeline_DisposeCalledBeforeStartLoggerLeftOpen_PreventsSubsequeuntExecution()
      {
         //This test is essentially the same as processPipeline_DisposeCalledBeforeStart_PreventsSubsequeuntExecution above
         // it only differs in CloseLoggerOnDispose set to false (non-default) to allow logging after orchestrator's disposal.

         //arrange
         _config.CloseLoggerOnDispose = false;
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPO = new PrivateObject(orchestrator);
         var transformingBlock = (TransformManyBlock<KeyValCluster, KeyValCluster>)orchestratorPO.GetField("_transformingBlock");
         transformingBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         var logger = (MockLogger)_config.Logger;

         //act
         orchestrator.Dispose();

         //assert
         _resultingClusters.Count.Should().Be(0);

         //act2
         var executionTask = orchestrator.ExecuteAsync();  //ExecuteAsync after Dispose
         var results = executionTask.Result;

         //assert2
         results.CompletionStatus.Should().Be(CompletionStatus.InvalidAttempt);
         results.RowsRead.Should().Be(0);
         results.ClustersRead.Should().Be(0);
         results.RowsWritten.Should().Be(0);
         results.ClustersWritten.Should().Be(0);

         logger.Results.Count.Should().Be(2);
         //Tuple contained in MockLogger.Results: Item1=severity, Item2=message, Item3=entry sent to logger
         //Note that due to the CloseLoggerOnDispose = false setting, no attempt to Dispose the logger is made.
         //Therefore, the logging can extend beyond the disposal of the orchestrator object.
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logEntry = logger.Results[1];
         logEntry.Item1.Should().Be(LogEntrySeverity.Fatal);
         logEntry.Item2.Contains("Invalid attempt to start execution ").Should().BeTrue();
      }


      [TestMethod]
      public void processPipeline_TwoExecuteAsyncCalls_2ndCallFails()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.IntakeSupplier = _inLine;
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.TransformerType = TransformerType.Recordbound;
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPO = new PrivateObject(orchestrator);
         var transformingBlock = (TransformManyBlock<KeyValCluster, KeyValCluster>)orchestratorPO.GetField("_transformingBlock");
         transformingBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         var logger = (MockLogger)_config.Logger;

         //act
         var execTask1 = orchestrator.ExecuteAsync();
         var execTask2 = orchestrator.ExecuteAsync();
         var results1 = execTask1.Result;
         var results2 = execTask2.Result;

         //assert
         results1.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         results1.RowsRead.Should().Be(6);
         results1.ClustersRead.Should().Be(5);
         results1.RowsWritten.Should().Be(0);  //no output invoked during the test
         results1.ClustersWritten.Should().Be(0);

         _resultingClusters.Count.Should().Be(5);   //this is from execTask1
         _resultingClusters[0].Count.Should().Be(1);
         _resultingClusters[1].Count.Should().Be(1);
         _resultingClusters[4].Count.Should().Be(1);

         results2.CompletionStatus.Should().Be(CompletionStatus.InvalidAttempt);
         results2.RowsRead.Should().Be(0);
         results2.ClustersRead.Should().Be(0);
         results2.RowsWritten.Should().Be(0);
         results2.ClustersWritten.Should().Be(0);

         logger.Results.Count.Should().Be(2);  //this is from execTask2    //first message is log title
         //Tuple contained in MockLogger.Results: Item1=severity, Item2=message, Item3=entry sent to logger
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);  //title box
         var logEntry = logger.Results[1];
         logEntry.Item1.Should().Be(LogEntrySeverity.Fatal);
         logEntry.Item2.Contains("Invalid attempt to start execution ").Should().BeTrue();

      }



   }
}