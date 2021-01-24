//EtlOrchestrator_tests_PropertyBin.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Generic;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_PropertyBin
   {
      private readonly OrchestratorConfig _config;
      private IEnumerable<string> _intakeLines()
      {
         yield return "FNAME,LNAME";
         yield return "Loren,Asar";
         yield return "Lara,Gudroe";
         yield return "Shawna,Palaspas";
      }

      //Array of property bin objects to assert on:
      private readonly Dictionary<string, IDictionary<string, object>> _pbSnapshots;
      private readonly object _locker;

      public EtlOrchestrator_tests_PropertyBin()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Delimited
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.ConcurrencyLevel = 3;
         _config.TransformerType = TransformerType.Clusterbound;

         _pbSnapshots = new Dictionary<string, IDictionary<string, object>>();
         _locker = new object();
      }


      /// <summary>
      /// Helper method to save a clone of the property bin object to _pbSnapshots
      /// </summary>
      /// <param name="context"></param>
      /// <param name="pbToSave"></param>
      private void SavePbSnapshot(string context, IDictionary<string, object> pbToSave)
      {
         lock (_locker) { _pbSnapshots.Add(context, new Dictionary<string, object>(pbToSave)); }
      }


      [Fact]
      public void PropertyBin_NoEntitiesDefinedInConfig_AllNull()
      {
         //arrange
         bool pbIsNotNull = false;
         _config.RecordInitiator = (r, tb) =>
         {
            pbIsNotNull |= r.PropertyBin != null;
            return true;
         };
         _config.HeadersInFirstInputRow = true;
         _config.ClusterMarker = (r, pr, n) =>
         {
            pbIsNotNull |= r.PropertyBin != null;
            if (pr != null) pbIsNotNull |= pr.PropertyBin != null;
            return true;
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            pbIsNotNull |= r.PropertyBin != null;
            return r;
         };
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c =>
         {
            pbIsNotNull |= c.PropertyBin != null;
            foreach (var r in c.Records)
            {
               pbIsNotNull |= r.PropertyBin != null;
            }
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(3);

         pbIsNotNull.Should().BeFalse();
      }


      [Fact]
      public void PropertyBin_AttachedToRecords_CorrectData()
      {
         //arrange
         bool clstrPbIsNotNull = false;  //to assure no cstr-level bin in present (only rec-level)
         _config.PropertyBinEntities = PropertyBinAttachedTo.Records;
         //Note: PB for each of the 3 records will have 2 elements: RecNo and ItemCnt
         //      PB for rec#1 will also have a "COUNTER" element incremented at each function
         _config.RecordInitiator = (r, tb) =>
         {  //2 PB elements at each of the 3 records
            r.PropertyBin.Add("RecNo", r.RecNo);
            r.PropertyBin.Add("ItemCnt", r.Count);
            if (r.RecNo == 1)
            {
               r.PropertyBin.Add("COUNTER", 0);
               r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //1
            }
            SavePbSnapshot($"AtRI_R#{r.RecNo}", r.PropertyBin);
            return true;
         };
         _config.ClusterMarker = (r, pr, n) =>
         {
            if (r.RecNo == 1) r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //2
            if (r.RecNo == 2) pr.PropertyBin["COUNTER"] = (int)(pr.PropertyBin["COUNTER"]) + 1;  //3
            return true;
         };
         _config.ClusterboundTransformer = c =>
         {
            if (c.StartRecNo == 1) c.Records[0].PropertyBin["COUNTER"] = (int)(c.Records[0].PropertyBin["COUNTER"]) + 1;  //4
            clstrPbIsNotNull |= c.PropertyBin != null;
            return c.GetClone();  //cloning retains the same PB
         };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) =>
         {
            if (r.RecNo == 1)
            {
               r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //5
               c.Records[0].PropertyBin["COUNTER"] = (int)(c.Records[0].PropertyBin["COUNTER"]) + 1;  //6
            }
            clstrPbIsNotNull |= c.PropertyBin != null;
            SavePbSnapshot($"AtRR_R#{r.RecNo}", r.PropertyBin);
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(3);

         clstrPbIsNotNull.Should().BeFalse();  //no bin attached to clstr in any event

         _pbSnapshots.Count.Should().Be(6);  //3 at initiator + 3 at router 

         var pb = _pbSnapshots["AtRI_R#1"];
         pb.Count.Should().Be(3);
         pb["RecNo"].Should().Be(1);
         pb["ItemCnt"].Should().Be(2);
         pb["COUNTER"].Should().Be(1);

         pb = _pbSnapshots["AtRI_R#2"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(2);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out object dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRI_R#3"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(3);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_R#1"];
         pb.Count.Should().Be(3);
         pb["RecNo"].Should().Be(1);
         pb["ItemCnt"].Should().Be(2);
         pb["COUNTER"].Should().Be(6);

         pb = _pbSnapshots["AtRR_R#2"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(2);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();
      }


      [Fact]
      public void PropertyBin_AttachedToClusters_CorrectData()
      {
         //arrange
         bool recPbIsNotNull = false;  //to assure no rec-level bin in present (only clstr-level)
         _config.PropertyBinEntities = PropertyBinAttachedTo.Clusters;
         //Note: PB for each of the 3 clusters will have a single element: ClstrNo
         //      PB for clstr#2 will also have a "COUNTER" element incremented at each function
         _config.RecordInitiator = (r, tb) =>
         {  //no PB at record level
            recPbIsNotNull |= r.PropertyBin != null;
            return true;
         };
         _config.ClusterMarker = (r, pr, n) =>
         {
            recPbIsNotNull |= r.PropertyBin != null;
            recPbIsNotNull |= pr?.PropertyBin != null;
            return true;
         };
         _config.ClusterboundTransformer = c =>
         {
            c.PropertyBin.Add("ClstrNo", c.ClstrNo);
            if (c.ClstrNo == 2)
            {
               c.PropertyBin.Add("COUNTER", 0);
               c.PropertyBin["COUNTER"] = (int)(c.PropertyBin["COUNTER"]) + 1;  //1
            }
            SavePbSnapshot($"AtCT_C#{c.ClstrNo}", c.PropertyBin);
            return c;
         };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) =>
         {
            if (c.ClstrNo == 2) c.PropertyBin["COUNTER"] = (int)(c.PropertyBin["COUNTER"]) + 1;  //2
            SavePbSnapshot($"AtRR_C#{c.ClstrNo}", c.PropertyBin);
            recPbIsNotNull |= r.PropertyBin != null;
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(3);

         recPbIsNotNull.Should().BeFalse();  //no bin attached to records in any event

         _pbSnapshots.Count.Should().Be(6);  //3 at transformer + 3 at router 

         var pb = _pbSnapshots["AtCT_C#1"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(1);
         pb.TryGetValue("COUNTER", out object dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtCT_C#2"];
         pb.Count.Should().Be(2);
         pb["ClstrNo"].Should().Be(2);
         pb["COUNTER"].Should().Be(1);

         pb = _pbSnapshots["AtCT_C#3"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(3);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_C#1"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(1);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_C#2"];
         pb.Count.Should().Be(2);
         pb["ClstrNo"].Should().Be(2);
         pb["COUNTER"].Should().Be(2);
      }


      [Fact]
      public void PropertyBin_AttachedToBothRecsAndClstrs_CorrectData()
      {
         //arrange
         _config.PropertyBinEntities = PropertyBinAttachedTo.Records | PropertyBinAttachedTo.Clusters;
         //Note: PB for each of the 3 records will have 2 elements: RecNo and ItemCnt
         //      PB for rec#3 will also have a "COUNTER" element incremented at each function
         //      PB for each of the 3 clusters will have a single element: ClstrNo
         //      PB for clstr#1 will also have a "COUNTER" element incremented at each function
         _config.RecordInitiator = (r, tb) =>
         {  //2 PB elements at each of the 3 records
            r.PropertyBin.Add("RecNo", r.RecNo);
            r.PropertyBin.Add("ItemCnt", r.Count);
            if (r.RecNo == 3)
            {
               r.PropertyBin.Add("COUNTER", 0);
               r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //1
            }
            SavePbSnapshot($"AtRI_R#{r.RecNo}", r.PropertyBin);
            return true;
         };
         _config.ClusterMarker = (r, pr, n) =>
         {
            if (r.RecNo == 3) r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //2
            return true;
         };
         _config.ClusterboundTransformer = c =>
         {
            if (c.StartRecNo == 3) c.Records[0].PropertyBin["COUNTER"] = (int)(c.Records[0].PropertyBin["COUNTER"]) + 1;  //3
            c.PropertyBin.Add("ClstrNo", c.ClstrNo);
            if (c.ClstrNo == 1)
            {
               c.PropertyBin.Add("COUNTER", 0);
               c.PropertyBin["COUNTER"] = (int)(c.PropertyBin["COUNTER"]) + 1;  //1
            }
            SavePbSnapshot($"AtCT_C#{c.ClstrNo}", c.PropertyBin);
            return c.GetClone();  //cloning retains the same PB
         };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) =>
         {
            if (r.RecNo == 3)
            {
               r.PropertyBin["COUNTER"] = (int)(r.PropertyBin["COUNTER"]) + 1;  //4
               c.Records[0].PropertyBin["COUNTER"] = (int)(c.Records[0].PropertyBin["COUNTER"]) + 1;  //5
            }
            SavePbSnapshot($"AtRR_R#{r.RecNo}", r.PropertyBin);
            if (c.ClstrNo == 1) c.PropertyBin["COUNTER"] = (int)(c.PropertyBin["COUNTER"]) + 1;  //2
            SavePbSnapshot($"AtRR_C#{c.ClstrNo}", c.PropertyBin);
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(3);

         _pbSnapshots.Count.Should().Be(12);  //3 at initiator + 3 at transformer + 6 at router 

         var pb = _pbSnapshots["AtRI_R#1"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(1);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out object dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRI_R#2"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(2);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRI_R#3"];
         pb.Count.Should().Be(3);
         pb["RecNo"].Should().Be(3);
         pb["ItemCnt"].Should().Be(2);
         pb["COUNTER"].Should().Be(1);

         pb = _pbSnapshots["AtCT_C#1"];
         pb.Count.Should().Be(2);
         pb["ClstrNo"].Should().Be(1);
         pb["COUNTER"].Should().Be(1);

         pb = _pbSnapshots["AtCT_C#2"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtCT_C#3"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(3);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_R#1"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(1);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_R#2"];
         pb.Count.Should().Be(2);
         pb["RecNo"].Should().Be(2);
         pb["ItemCnt"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_R#3"];
         pb.Count.Should().Be(3);
         pb["RecNo"].Should().Be(3);
         pb["ItemCnt"].Should().Be(2);
         pb["COUNTER"].Should().Be(5);

         pb = _pbSnapshots["AtRR_C#1"];
         pb.Count.Should().Be(2);
         pb["ClstrNo"].Should().Be(1);
         pb["COUNTER"].Should().Be(2);

         pb = _pbSnapshots["AtRR_C#2"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(2);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();

         pb = _pbSnapshots["AtRR_C#3"];
         pb.Count.Should().Be(1);
         pb["ClstrNo"].Should().Be(3);
         pb.TryGetValue("COUNTER", out dummy).Should().BeFalse();
         dummy.Should().BeNull();
      }

   }
}
