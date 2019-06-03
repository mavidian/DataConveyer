//MultipleTargets_tests.cs
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


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataConveyer_tests.Output
{
   /// <summary>
   /// These are in fact integration tests (of the entire pipeline)
   /// </summary>
   [TestClass]
   public class MultipleTargets_tests
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
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=123";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary\",@pNUM=223";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary\",@pNUM=323";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Susan\",@pNUM=423";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Maria\",@pNUM=523";
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=623";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Jane\",@pNUM=723";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Jill\",@pNUM=823";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Betty\",@pNUM=923";
      }

      //Result of the tests are held here:
      List<string>[] _resultingLines;  //container of the test results (10 targets)


      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         //prepare extraction of the results from the pipeline
         _resultingLines = new List<string>[10];
         for (int i = 0; i < 10; i++)
         {
            _resultingLines[i] = new List<string>();  //note that indexes 0..9 correspond to target numbers 1..10
         }

         var sn = 0; //to assign sourceNo in a round-robin fashion: 1,2,3,1,2,3,1,2,3
         _inLines = _intakeLines().Select(l => l.ToExternalTuple(sn++ % 3 + 1)).ToList();
      }


      [TestMethod]
      public void processSourcesToTargets_NoLeadersHeaderTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(3);
         lines[0].Should().Be("XYZ         Mary, Ann           123       ");
         lines[1].Should().Be("ABCD        Susan               423       ");
         lines[2].Should().Be("ABCD        Jane                723       ");

         lines = _resultingLines[1];  //target 2
         lines.Count.Should().Be(3);
         lines[0].Should().Be("ABCD        Mary                223       ");
         lines[1].Should().Be("ABCD        Maria               523       ");
         lines[2].Should().Be("ABCD        Jill                823       ");

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(3);
         lines[0].Should().Be("ABCD        Mary                323       ");
         lines[1].Should().Be("XYZ         Mary, Ann           623       ");
         lines[2].Should().Be("ABCD        Betty               923       ");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5
         _resultingLines[5].Count.Should().Be(0);  //target 6
         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processTargetsPerCluster_NoLeadersHeaderTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => c.ClstrNo * 3;   //1st cluster (5 recs) goes to target 3, the 2nd (4 recs) to 6
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(0);

         _resultingLines[1].Count.Should().Be(0);    //target 2

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(5);
         lines[0].Should().Be("XYZ         Mary, Ann           123       ");
         lines[1].Should().Be("ABCD        Mary                223       ");
         lines[2].Should().Be("ABCD        Mary                323       ");
         lines[3].Should().Be("ABCD        Susan               423       ");
         lines[4].Should().Be("ABCD        Maria               523       ");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5


         lines = _resultingLines[5];  //target 6
         lines.Count.Should().Be(4);
         lines[0].Should().Be("XYZ         Mary, Ann           623       ");
         lines[1].Should().Be("ABCD        Jane                723       ");
         lines[2].Should().Be("ABCD        Jill                823       ");
         lines[3].Should().Be("ABCD        Betty               923       ");

         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processSourcesToTargets_NonRepeatedHeaderNoLeadersTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.RepeatOutputHeaders = false;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(10);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(4);
         lines[0].Should().Be("RECTYPE     NAME                NUM       ");
         lines[1].Should().Be("XYZ         Mary, Ann           123       ");
         lines[2].Should().Be("ABCD        Susan               423       ");
         lines[3].Should().Be("ABCD        Jane                723       ");

         lines = _resultingLines[1];  //target 2
         lines.Count.Should().Be(3);
         lines[0].Should().Be("ABCD        Mary                223       ");
         lines[1].Should().Be("ABCD        Maria               523       ");
         lines[2].Should().Be("ABCD        Jill                823       ");

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(3);
         lines[0].Should().Be("ABCD        Mary                323       ");
         lines[1].Should().Be("XYZ         Mary, Ann           623       ");
         lines[2].Should().Be("ABCD        Betty               923       ");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5
         _resultingLines[5].Count.Should().Be(0);  //target 6
         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processSourcesToTargets_RepeatedHeaderNoLeadersTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.RepeatOutputHeaders = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(12);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(4);
         lines[0].Should().Be("RECTYPE     NAME                NUM       ");
         lines[1].Should().Be("XYZ         Mary, Ann           123       ");
         lines[2].Should().Be("ABCD        Susan               423       ");
         lines[3].Should().Be("ABCD        Jane                723       ");

         lines = _resultingLines[1];  //target 2
         lines.Count.Should().Be(4);
         lines[0].Should().Be("RECTYPE     NAME                NUM       ");
         lines[1].Should().Be("ABCD        Mary                223       ");
         lines[2].Should().Be("ABCD        Maria               523       ");
         lines[3].Should().Be("ABCD        Jill                823       ");

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(4);
         lines[0].Should().Be("RECTYPE     NAME                NUM       ");
         lines[1].Should().Be("ABCD        Mary                323       ");
         lines[2].Should().Be("XYZ         Mary, Ann           623       ");
         lines[3].Should().Be("ABCD        Betty               923       ");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5
         _resultingLines[5].Count.Should().Be(0);  //target 6
         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processSourcesToTargets_RepeatedLeadersNonRepeatedHeaderNoTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.RepeatOutputHeaders = false;
         _config.LeaderContents = "1st leader line\r\n2nd leader line ending at 2 spaces  ";  //2 lines

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(16);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(6);
         lines[0].Should().Be("1st leader line");
         lines[1].Should().Be("2nd leader line ending at 2 spaces  ");
         lines[2].Should().Be("RECTYPE     NAME                NUM       ");
         lines[3].Should().Be("XYZ         Mary, Ann           123       ");
         lines[4].Should().Be("ABCD        Susan               423       ");
         lines[5].Should().Be("ABCD        Jane                723       ");

         lines = _resultingLines[1];  //target 2
         lines.Count.Should().Be(5);
         lines[0].Should().Be("1st leader line");
         lines[1].Should().Be("2nd leader line ending at 2 spaces  ");
         lines[2].Should().Be("ABCD        Mary                223       ");
         lines[3].Should().Be("ABCD        Maria               523       ");
         lines[4].Should().Be("ABCD        Jill                823       ");

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(5);
         lines[0].Should().Be("1st leader line");
         lines[1].Should().Be("2nd leader line ending at 2 spaces  ");
         lines[2].Should().Be("ABCD        Mary                323       ");
         lines[3].Should().Be("XYZ         Mary, Ann           623       ");
         lines[4].Should().Be("ABCD        Betty               923       ");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5
         _resultingLines[5].Count.Should().Be(0);  //target 6
         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processTargetsPerCluster_NonRepeatedLeadersRepeatedHeaderNoTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => 8 - c.ClstrNo * 2;   //1st cluster (5 recs) goes to target 6, the 2nd (4 recs) to 4
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.LeaderContents = "Single line leader";
         _config.RepeatLeaders = false;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(12);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(0);

         _resultingLines[1].Count.Should().Be(0);   //target 2

         _resultingLines[2].Count.Should().Be(0);   //target 3

         lines = _resultingLines[3];  //target 4
         lines.Count.Should().Be(5);
         lines[0].Should().Be("RECTYPE     NAME                NUM       ");
         lines[1].Should().Be("XYZ         Mary, Ann           623       ");
         lines[2].Should().Be("ABCD        Jane                723       ");
         lines[3].Should().Be("ABCD        Jill                823       ");
         lines[4].Should().Be("ABCD        Betty               923       ");

         _resultingLines[4].Count.Should().Be(0);  //target 5

         lines = _resultingLines[5];  //target 6  - note that this target is written to first (!)
         lines.Count.Should().Be(7);
         lines[0].Should().Be("Single line leader");
         lines[1].Should().Be("RECTYPE     NAME                NUM       ");
         lines[2].Should().Be("XYZ         Mary, Ann           123       ");
         lines[3].Should().Be("ABCD        Mary                223       ");
         lines[4].Should().Be("ABCD        Mary                323       ");
         lines[5].Should().Be("ABCD        Susan               423       ");
         lines[6].Should().Be("ABCD        Maria               523       ");

         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processTargetsPerCluster_NonRepeatedTrailersNoLeadersHeader_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => 8 - c.ClstrNo * 2;   //1st cluster (5 recs) goes to target 6, the 2nd (4 recs) to 4
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = false;
         _config.TrailerContents = "First trailer line\r\nSecond trailer line with 3 spaces   \r\n Third trailer with space before and after ";
         _config.RepeatTrailers = false;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(12);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(0);

         _resultingLines[1].Count.Should().Be(0);   //target 2

         _resultingLines[2].Count.Should().Be(0);   //target 3

         lines = _resultingLines[3];  //target 4  - note that this target is last written to
         lines.Count.Should().Be(7);
         lines[0].Should().Be("XYZ         Mary, Ann           623       ");
         lines[1].Should().Be("ABCD        Jane                723       ");
         lines[2].Should().Be("ABCD        Jill                823       ");
         lines[3].Should().Be("ABCD        Betty               923       ");
         lines[4].Should().Be("First trailer line");
         lines[5].Should().Be("Second trailer line with 3 spaces   ");
         lines[6].Should().Be(" Third trailer with space before and after ");

         _resultingLines[4].Count.Should().Be(0);  //target 5

         lines = _resultingLines[5];  //target 6
         lines.Count.Should().Be(5);
         lines[0].Should().Be("XYZ         Mary, Ann           123       ");
         lines[1].Should().Be("ABCD        Mary                223       ");
         lines[2].Should().Be("ABCD        Mary                323       ");
         lines[3].Should().Be("ABCD        Susan               423       ");
         lines[4].Should().Be("ABCD        Maria               523       ");

         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }


      [TestMethod]
      public void processTargetsPerCluster_NonRepeatedLeadersRepeatedTrailersNoHeader_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => 8 - c.ClstrNo * 2;   //1st cluster (5 recs) goes to target 6, the 2nd (4 recs) to 4
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = false;
         _config.LeaderContents = "Single line leader";
         _config.RepeatLeaders = false;
         _config.TrailerContents = "Single line trailer";
         _config.RepeatTrailers = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(12);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(0);

         _resultingLines[1].Count.Should().Be(0);   //target 2

         _resultingLines[2].Count.Should().Be(0);   //target 3

         lines = _resultingLines[3];  //target 4  - note that this target is last written to
         lines.Count.Should().Be(5);
         lines[0].Should().Be("XYZ         Mary, Ann           623       ");
         lines[1].Should().Be("ABCD        Jane                723       ");
         lines[2].Should().Be("ABCD        Jill                823       ");
         lines[3].Should().Be("ABCD        Betty               923       ");
         lines[4].Should().Be("Single line trailer");

         _resultingLines[4].Count.Should().Be(0);  //target 5

         lines = _resultingLines[5];  //target 6
         lines.Count.Should().Be(7);
         lines[0].Should().Be("Single line leader");
         lines[1].Should().Be("XYZ         Mary, Ann           123       ");
         lines[2].Should().Be("ABCD        Mary                223       ");
         lines[3].Should().Be("ABCD        Mary                323       ");
         lines[4].Should().Be("ABCD        Susan               423       ");
         lines[5].Should().Be("ABCD        Maria               523       ");
         lines[6].Should().Be("Single line trailer");

         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }



      [TestMethod]
      public void processSourcesToTargets_RepeatedLeadersHeaderTrailers_CorrectDataOnTargets()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputConsumer = (t, gc) => { if (t != null) _resultingLines[t.Item2 - 1].Add(t.Item1.Text); };  // place the output lines on the list for the appropriate target to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.RepeatOutputHeaders = true;
         _config.LeaderContents = "Single line leader";
         _config.RepeatLeaders = true;
         _config.TrailerContents = "Single line trailer";
         _config.RepeatTrailers = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(18);
         counts.ClustersWritten.Should().Be(2);

         var lines = _resultingLines[0];  //target 1
         lines.Count.Should().Be(6);
         lines[0].Should().Be("Single line leader");
         lines[1].Should().Be("RECTYPE     NAME                NUM       ");
         lines[2].Should().Be("XYZ         Mary, Ann           123       ");
         lines[3].Should().Be("ABCD        Susan               423       ");
         lines[4].Should().Be("ABCD        Jane                723       ");
         lines[5].Should().Be("Single line trailer");

         lines = _resultingLines[1];  //target 2
         lines.Count.Should().Be(6);
         lines[0].Should().Be("Single line leader");
         lines[1].Should().Be("RECTYPE     NAME                NUM       ");
         lines[2].Should().Be("ABCD        Mary                223       ");
         lines[3].Should().Be("ABCD        Maria               523       ");
         lines[4].Should().Be("ABCD        Jill                823       ");
         lines[5].Should().Be("Single line trailer");

         lines = _resultingLines[2];  //target 3
         lines.Count.Should().Be(6);
         lines[0].Should().Be("Single line leader");
         lines[1].Should().Be("RECTYPE     NAME                NUM       ");
         lines[2].Should().Be("ABCD        Mary                323       ");
         lines[3].Should().Be("XYZ         Mary, Ann           623       ");
         lines[4].Should().Be("ABCD        Betty               923       ");
         lines[5].Should().Be("Single line trailer");

         _resultingLines[3].Count.Should().Be(0);  //target 4
         _resultingLines[4].Count.Should().Be(0);  //target 5
         _resultingLines[5].Count.Should().Be(0);  //target 6
         _resultingLines[6].Count.Should().Be(0);  //target 7
         _resultingLines[7].Count.Should().Be(0);  //target 8
         _resultingLines[8].Count.Should().Be(0);  //target 9
         _resultingLines[9].Count.Should().Be(0);  //target 10
      }

   }
}
