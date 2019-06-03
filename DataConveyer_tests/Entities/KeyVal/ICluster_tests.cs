//ICluster_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class ICluster_tests
   {
      private OrchestratorConfig _config;

      private TypeDefinitions _typeDefs;
      private ICluster _cluster;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();
         _config.AllowTransformToAlterFields = true;
         _config.PropertyBinEntities = PropertyBinAttachedTo.Clusters;

         // simple type definitions, everything string, except for fields starting with I_ (int)
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("I_") ? ItemType.Int : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();
         _typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         var items1 = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                    KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                    KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         var items2 = new IItem[] { KeyValItem.CreateItem("I_#", 15, _typeDefs),
                                    KeyValItem.CreateItem("Fld1", "data1", _typeDefs)};

         var recs = new KeyValRecord[] { new KeyValRecord(items1, 16, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem),
                                         new KeyValRecord(items2, 17, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem) };

         _cluster = new KeyValCluster(recs, 9, 16, 1, null, new Dictionary<string, object>(), _typeDefs, _config, null);  //clstr# 9 starting at rec# 16; AllowTransformToAlterFields is true
      }

      [TestMethod]
      public void BasicProperties_SimpleValues_CorrectData()
      {
         //arrange
         var clstr = _cluster;

         //act
         //assert
         clstr.Count.Should().Be(2);
         clstr.ClstrNo.Should().Be(9);
         clstr.StartRecNo.Should().Be(16);

         var recds = clstr.Records;
         recds.Count.Should().Be(2);
         recds[0].Should().BeSameAs(clstr[0]);
         recds[1].Should().BeSameAs(clstr[1]);

         var rec = clstr[0];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.Keys[0].Should().Be("IDCD_ID");
         rec[0].Should().Be("71941");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);

         rec = clstr[1];
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(17);
         rec.Keys[0].Should().Be("I_#");
         rec[0].Should().Be(15);
         rec["I_#"].Should().Be(15);
         rec.Keys[1].Should().Be("Fld1");
         rec["Fld1"].Should().Be("data1");
      }


      [TestMethod]
      public void RemoveRecord_SimpleValues_CorrectData()
      {
         //arrange
         var clstr = _cluster;

         //act
         clstr.RemoveRecord(0);

         //assert
         clstr.Count.Should().Be(1);
         clstr.ClstrNo.Should().Be(9);
         clstr.StartRecNo.Should().Be(16);

         var rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(17);
         rec.Keys[0].Should().Be("I_#");
         rec[0].Should().Be(15);
         rec["I_#"].Should().Be(15);
         rec.Keys[1].Should().Be("Fld1");
         rec["Fld1"].Should().Be("data1");
      }


      [TestMethod]
      public void AddRecord_SimpleValues_CorrectData()
      {
         //arrange
         var clstr = _cluster;

         //act
         clstr.AddRecord(clstr[0].GetClone());

         //assert
         clstr.Count.Should().Be(3);
         clstr.ClstrNo.Should().Be(9);
         clstr.StartRecNo.Should().Be(16);

         var rec = clstr[0];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.Keys[0].Should().Be("IDCD_ID");
         rec[0].Should().Be("71941");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);

         rec = clstr[1];
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(17);
         rec.Keys[0].Should().Be("I_#");
         rec[0].Should().Be(15);
         rec["I_#"].Should().Be(15);
         rec.Keys[1].Should().Be("Fld1");
         rec["Fld1"].Should().Be("data1");

         rec = clstr[2];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.Keys[0].Should().Be("IDCD_ID");
         rec[0].Should().Be("71941");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
      }


      [TestMethod]
      public void GetRecord_ValidIndex_SameAsIndexer()
      {
         //arrange

         //act
         var record = _cluster.GetRecord(1);

         //assert
         record.Should().BeSameAs(_cluster[1]);
         record.Should().BeSameAs(_cluster.Records[1]);
      }


      [TestMethod]
      public void GetRecord_OutOfRangeIndex_ReturnNull()
      {
         //arrange

         //act
         var bogusRecord = _cluster.GetRecord(2);

         //assert
         bogusRecord.Should().BeNull();
      }


      [TestMethod]
      public void GetClone_SimpleValues_SameData()
      {
         //arrange

         //act
         var clone = _cluster.GetClone();

         //assert
         clone.Count.Should().Be(2);
         clone.ClstrNo.Should().Be(9);
         clone.StartRecNo.Should().Be(16);

         var rec = clone[0];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.Keys[0].Should().Be("IDCD_ID");
         rec[0].Should().Be("71941");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
      }


      [TestMethod]
      public void GetClone_PropertyBin_SameInstance()
      {
         //arrange

         //act
         var clone = _cluster.GetClone();

         //assert
         clone.PropertyBin.Should().NotBeNull();
         clone.PropertyBin.Should().BeSameAs(_cluster.PropertyBin);
      }


      [TestMethod]
      public void GetEmptyClone_SimpleValues_EmptyData()
      {
         //arrange

         //act
         var clone = _cluster.GetEmptyClone();

         //assert
         clone.Count.Should().Be(0);
         clone.ClstrNo.Should().Be(9);
         clone.StartRecNo.Should().Be(16);

         clone.Records.Count.Should().Be(0);

         clone.GetRecord(0).Should().BeNull();
      }


      [TestMethod]
      public void GetEmptyClone_DisallowAlterFields_EmptyData()
      {
         //arrange
         var items1 = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                    KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                    KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         var items2 = new IItem[] { KeyValItem.CreateItem("I_#", 15, _typeDefs),
                                    KeyValItem.CreateItem("Fld1", "data1", _typeDefs)};

         var recs = new KeyValRecord[] { new KeyValRecord(items1, 16, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem),
                                         new KeyValRecord(items2, 17, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem) };

         _config.AllowTransformToAlterFields = false;  //it was set to true in test Initialize method
         var cluster = new KeyValCluster(recs, 9, 16, 1, null, null, _typeDefs, _config, null);  //clstr# 9 starting at rec# 16; AllowTransformToAlterFields is false

         //act
         var clone = cluster.GetEmptyClone();

         //assert
         clone.Count.Should().Be(0);
         clone.ClstrNo.Should().Be(9);
         clone.StartRecNo.Should().Be(16);

         clone.Records.Count.Should().Be(0);

         clone.GetRecord(0).Should().BeNull();
      }


      [TestMethod]
      public void GetEmptyClone_PropertyBin_SameInstance()
      {
         //arrange

         //act
         var clone = _cluster.GetEmptyClone();

         //assert
         clone.PropertyBin.Should().NotBeNull();
         clone.PropertyBin.Should().BeSameAs(_cluster.PropertyBin);
      }

   }
}
