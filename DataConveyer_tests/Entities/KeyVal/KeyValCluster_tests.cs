//KeyValCluster_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class KeyValCluster_tests
   {
      private OrchestratorConfig _config;

      private TypeDefinitions _typeDefs;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();
         _config.AllowTransformToAlterFields = true;

         // simple type definitions, everything string, except for fields starting with I_ (int)
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("I_") ? ItemType.Int : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();
         _typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);
      }

      [TestMethod]
      public void Ctor_SimpleValues_CorrectData()
      {
         //arrange
         var items1 = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                    KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                    KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         var items2 = new IItem[] { KeyValItem.CreateItem("I_#", 15, _typeDefs),
                                    KeyValItem.CreateItem("Fld1", "data1", _typeDefs)};

         var recs = new KeyValRecord[] { new KeyValRecord(items1, 16, 7, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem),
                                         new KeyValRecord(items2, 17, 8, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem) };

         //act
         var clstr = new KeyValCluster(recs, 9, 16, 78, null, null, _typeDefs, _config, null);  //clstr# 9 starting at rec# 16; AllowTransformToAlterFields is true
                                                                                                // note that startSourceNo 78 does not match the 1st record (7) - not a real scenario, but a good text

         //assert
         clstr.Count.Should().Be(2);
         clstr.ClstrNo.Should().Be(9);
         clstr.StartRecNo.Should().Be(16);
         clstr.StartSourceNo.Should().Be(78);

         var rec = clstr[0];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(7);
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
         rec.SourceNo.Should().Be(8);
         rec.Keys[0].Should().Be("I_#");
         rec[0].Should().Be(15);
         rec["I_#"].Should().Be(15);
         rec.Keys[1].Should().Be("Fld1");
         rec["Fld1"].Should().Be("data1");
      }


      [TestMethod]
      public void Clone_SimpleValues_SameData()
      {
         //arrange
         var items1 = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                    KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                    KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         var items2 = new IItem[] { KeyValItem.CreateItem("I_#", 15, _typeDefs),
                                    KeyValItem.CreateItem("Fld1", "data1", _typeDefs)};

         var recs = new KeyValRecord[] { new KeyValRecord(items1, 16, 7, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem),
                                         new KeyValRecord(items2, 17, 8, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem) };

         //act
         var clstr = new KeyValCluster(recs, 9, 16, 78, null, null, _typeDefs, _config, null);  //clstr# 9 starting at rec# 16; AllowTransformToAlterFields is true
                                                                                                // note that startSourceNo 78 does not match the 1st record (7) - not a real scenario, but a good text
         var clone = clstr.GetClone();

         //assert
         clone.Should().NotBeSameAs(clstr);
         clone.Count.Should().Be(2);
         clone.ClstrNo.Should().Be(9);
         clone.StartRecNo.Should().Be(16);
         clone.StartSourceNo.Should().Be(78);

         var rec = clone[0];
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(7);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.GetItem(0).Key.Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);

         rec = clone[1];
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(17);
         rec.SourceNo.Should().Be(8);
         rec.Keys[0].Should().Be("I_#");
         rec[0].Should().Be(15);
         rec.GetItem("I_#").Key.Should().Be("I_#");
         rec.Keys[1].Should().Be("Fld1");
         rec["Fld1"].Should().Be("data1");

      }


      [TestMethod]
      public void ObtainEmptyRecord_Simple_CorrectData()
      {
         //arrange
         var clstr = new KeyValCluster(new List<KeyValRecord>(), 6, 15, 4, null, null, _typeDefs, _config, null);

         //act
         var rec = clstr.ObtainEmptyRecord();

         //assert
         clstr.Count.Should().Be(0);  //still no records in cluster
         rec.Should().BeOfType<KeyValRecord>();
         rec.RecNo.Should().Be(15);
         rec.SourceNo.Should().Be(4);
         rec.Count.Should().Be(0);  //no items in record

         //act2
         rec.AddItem("K", "Val");

         //assert2
         rec.Count.Should().Be(1);
         rec["K"].Should().Be("Val");
      }


      [TestMethod]
      public void ObtainEmptyRecord_ThenCreateX12Segment_CorrectData()
      {
         //arrange
         var clstr = new KeyValCluster(new List<KeyValRecord>(), 0, 0, 1, null, null, _typeDefs, _config, null);

         //act
         var seg = clstr.ObtainEmptyRecord().CreateEmptyX12Segment("GS", 8);

         //assert
         clstr.Count.Should().Be(0);  //still no records in cluster
         seg.Should().BeOfType<KeyValRecord>();
         seg.RecNo.Should().Be(0);
         seg.SourceNo.Should().Be(1);
         seg.Count.Should().Be(9);  //8 + 1
         seg.Keys[0].Should().Be("Segment");
         seg[0].Should().Be("GS");
      }


      [TestMethod]
      public void ObtainEmptyRecord_ThenCreateX12SegmentWithContents_CorrectData()
      {
         //arrange
         var clstr = new KeyValCluster(new List<KeyValRecord>(), 0, 0, 1, null, null, _typeDefs, _config, null);
         var eRec = clstr.ObtainEmptyRecord();

         //act
         var seg = eRec.CreateFilledX12Segment("NM1*1P*1*GARDENER*JAMES****46*8189991234");
         //note that if undefined in config (or passed as a 2nd parm), CreateX12Segment uses * as field delimiter 

         //assert
         clstr.Count.Should().Be(0);  //still no records in cluster
         seg.Should().BeOfType<KeyValRecord>();
         seg.RecNo.Should().Be(0);
         seg.SourceNo.Should().Be(1);
         seg.Count.Should().Be(10);
         seg.Keys[0].Should().Be("Segment");
         seg[0].Should().Be("NM1");
         seg.Keys[1].Should().Be("Elem001");
         seg[1].Should().Be("1P");
         seg.Keys[6].Should().Be("Elem006");
         seg[6].Should().Be(string.Empty);
         seg.Keys[9].Should().Be("Elem009");
         seg[9].Should().Be("8189991234");

         //act2
         seg = eRec.CreateFilledX12Segment("HL^2^1^21^1", '^'); // here, explicit field delimiter

         //assert2
         clstr.Count.Should().Be(0);  //still no records in cluster
         seg.Should().BeOfType<KeyValRecord>();
         seg.RecNo.Should().Be(0);
         seg.SourceNo.Should().Be(1);
         seg.Count.Should().Be(5);
         seg.Keys[0].Should().Be("Segment");
         seg[0].Should().Be("HL");
         seg.Keys[1].Should().Be("Elem001");
         seg[1].Should().Be("2");
         seg.Keys[3].Should().Be("Elem003");
         seg[3].Should().Be("21");
         seg.Keys[4].Should().Be("Elem004");
         seg[4].Should().Be("1");
      }
   }
}
