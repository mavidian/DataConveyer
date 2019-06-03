//KeyValRecord_tests.cs
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
using System.Linq;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class KeyValRecord_tests
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
      public void KvRecCtor_SimpleValues_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         int recNo = 16;
         int sourceNo = 5;

         //act
         var rec = new KeyValRecord(items, recNo, sourceNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //assert
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(recNo);
         rec.SourceNo.Should().Be(sourceNo);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);

         // indexers from IRecord & IUntypedRecord interfaces:
         IRecord recR = rec;
         var recU = (IUntypedRecord)rec;
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         recR["IDCD_ID"].Should().Be("71941");
         recU["IDCD_ID"].Should().Be("71941");
         recR[2].Should().Be(243);
         recU[2].Should().Be("243");
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");

         // dynamic properties:
         dynamic recD = rec;
         Assert.AreEqual("71941", recD.IDCD_ID);  //FluentAssertions don't work with dynamic properties, e.g.'string' does not contain a definition for 'Should'
         Assert.AreEqual("blahblah", recD.blah);
         Assert.AreEqual(243, recD.I_num);
         Assert.IsNull(recD.BadKey);

         var keys = rec.Keys.ToList();
         keys.Count.Should().Be(3);
         keys[0].Should().Be("IDCD_ID");
         keys[1].Should().Be("blah");
         keys[2].Should().Be("I_num");

         var itms = rec.Items.ToList();
         itms.Count.Should().Be(3);
         itms[0].Key.Should().Be("IDCD_ID");
         itms[0].Value.Should().BeOfType(typeof(string));
         itms[0].Value.Should().Be("71941");
         itms[0].ItemDef.Type.Should().Be(ItemType.String);
         itms[1].Key.Should().Be("blah");
         itms[1].Value.Should().BeOfType(typeof(string));
         itms[1].Value.Should().Be("blahblah");
         itms[1].ItemDef.Type.Should().Be(ItemType.String);
         itms[2].Key.Should().Be("I_num");
         itms[2].Value.Should().BeOfType(typeof(int));
         itms[2].Value.Should().Be(243);
         itms[2].ItemDef.Type.Should().Be(ItemType.Int);
      }


      [TestMethod]
      public void KvRecCtor_NoItems_CorrectData()
      {
         //arrange
         var items = new List<IItem>();
         int recNo = 22;
         int srcNo = 4;

         //act
         var rec = new KeyValRecord(items, recNo, srcNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;  //n/a as there are no dynamic properties in this case

         //assert
         rec.Count.Should().Be(0);
         rec.RecNo.Should().Be(recNo);
         rec.SourceNo.Should().Be(srcNo);
         rec.Keys.Count.Should().Be(0);
         rec.Items.Count.Should().Be(0);
         rec[0].Should().BeNull();
         rec["any"].Should().BeNull();
         recR[0].Should().BeNull();
         recR["any"].Should().BeNull();
         recU[0].Should().BeNull();
         recU["any"].Should().BeNull();
      }


      [TestMethod]
      public void AssignValue_StringValue_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "origData", _typeDefs) };
         var rec = new KeyValRecord(items, 16, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         rec["blah"] = "NewValue";
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(3);
         rec[0].Should().Be("71941");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("NewValue");
         recR[1].Should().Be("NewValue");
         recU[1].Should().Be("NewValue");
         Assert.AreEqual("NewValue", recD.blah);

         //act2
         rec = new KeyValRecord(items, 16, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         recR = (IRecord)rec;
         recU = rec;
         recD = rec;
         recR["blah"] = "NewValueR";

         //assert2
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(3);
         rec[0].Should().Be("71941");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("NewValueR");
         recR[1].Should().Be("NewValueR");
         recU[1].Should().Be("NewValueR");
         Assert.AreEqual("NewValueR", recD.blah);

         //act3
         rec = new KeyValRecord(items, 16, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         recR = (IRecord)rec;
         recU = rec;
         recD = rec;
         recU["blah"] = "NewValueU";

         //assert3
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(3);
         rec[0].Should().Be("71941");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("NewValueU");
         recR[1].Should().Be("NewValueU");
         recU[1].Should().Be("NewValueU");
         Assert.AreEqual("NewValueU", recD.blah);

         //act4
         rec = new KeyValRecord(items, 16, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         recR = (IRecord)rec;
         recU = rec;
         recD = rec;
         recD.blah = "NewValueD";

         //assert4
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(3);
         rec[0].Should().Be("71941");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("NewValueD");
         recR[1].Should().Be("NewValueD");
         recU[1].Should().Be("NewValueD");
         Assert.AreEqual("NewValueD", recD.blah);
      }


      [TestMethod]
      public void AssignValue_TypedValue_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("blah", "origData", _typeDefs),
                                   KeyValItem.CreateItem("I_int", 88, _typeDefs) };
         var rec = new KeyValRecord(items, 16, 2, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         rec["I_int"] = 66;

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec[1].Should().Be(66);
         rec.Items[1].StringValue.Should().Be("66");
         recR[1].Should().Be(66);
         recR["I_int"].Should().Be(66);
         recU[1].Should().Be("66");
         recU["I_int"].Should().Be("66");
         Assert.AreEqual(66, recD.I_int);

         //act2
         recU["blah"] = "NewValue";

         //assert2
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec[0].Should().Be("NewValue");
         recR[0].Should().Be("NewValue");
         recR["blah"].Should().Be("NewValue");
         recU[0].Should().Be("NewValue");
         recU["blah"].Should().Be("NewValue");
         Assert.AreEqual("NewValue", recD.blah);

         //act3
         recR["blah"] = 150;

         //assert3
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec[0].Should().Be("150");
         recR[0].Should().Be("150");
         recR["blah"].Should().Be("150");
         recU[0].Should().Be("150");
         recU["blah"].Should().Be("150");
         Assert.AreEqual("150", recD.blah);

         //act4
         recD.I_int = "badNum";

         //assert4
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec[1].Should().Be(0);  //default(int)
         recR[1].Should().Be(0);
         recR["I_int"].Should().Be(0);
         recU[1].Should().Be("0");
         recU["I_int"].Should().Be("0");
         Assert.AreEqual(0, recD.I_int);
      }


      [TestMethod]
      public void GetClone_SimpleValues_SameData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 15, 4, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetClone();

         //assert
         clone.Should().NotBeSameAs(rec);
         clone.Count.Should().Be(3);
         clone.RecNo.Should().Be(15);
         clone.SourceNo.Should().Be(4);
         clone.Keys[0].Should().Be("IDCD_ID");
         clone.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         clone[1].Should().Be("blahblah");
         clone["blah"].Should().Be("blahblah");
         clone[2].Should().Be(243);
         clone["I_num"].Should().Be(243);

         var keys = clone.Keys.ToList();
         keys.Count.Should().Be(3);
         keys[0].Should().Be("IDCD_ID");
         keys[1].Should().Be("blah");
         keys[2].Should().Be("I_num");

         var itms = clone.Items.ToList();
         itms.Count.Should().Be(3);
         itms[0].Key.Should().Be("IDCD_ID");
         itms[0].Value.Should().BeOfType(typeof(string));
         itms[0].Value.Should().Be("71941");
         itms[0].ItemDef.Type.Should().Be(ItemType.String);
         itms[1].Key.Should().Be("blah");
         itms[1].Value.Should().BeOfType(typeof(string));
         itms[1].Value.Should().Be("blahblah");
         itms[1].ItemDef.Type.Should().Be(ItemType.String);
         itms[2].Key.Should().Be("I_num");
         itms[2].Value.Should().BeOfType(typeof(int));
         itms[2].Value.Should().Be(243);
         itms[2].ItemDef.Type.Should().Be(ItemType.Int);
      }


      [TestMethod]
      public void GetEmptyClone_SimpleValues_SameRecNoButEmptyData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 12, 11, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetEmptyClone();
         var cloneR = (IRecord)clone;  //this is no different, both clone and cloneR are of type IRecord
         var cloneU = (IUntypedRecord)clone;
         dynamic cloneD = clone;  //n/a as there are no dynamic properties in this case

         //assert
         clone.Should().NotBeSameAs(rec);
         clone.Count.Should().Be(0);
         clone.RecNo.Should().Be(12);
         clone.SourceNo.Should().Be(11);
         clone.Keys.Count.Should().Be(0);
         clone.Items.Count.Should().Be(0);
         clone[0].Should().BeNull();
         clone["any"].Should().BeNull();
         cloneR[0].Should().BeNull();
         cloneR["any"].Should().BeNull();
         cloneU[0].Should().BeNull();
         cloneU["any"].Should().BeNull();
      }


      [TestMethod]
      public void GetEmptyClone_DisallowAlterFields_SameRecNoButEmptyData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         _config.AllowTransformToAlterFields = false;  //it was set to true in test Initialize method
         var rec = new KeyValRecord(items, 16, 7, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetEmptyClone();   //note that this clone is unmaintainable (i.e. it will stay empty forever)
         var cloneR = (IRecord)clone;
         var cloneU = (IUntypedRecord)clone;

         //assert
         clone.Should().NotBeSameAs(rec);
         clone.Count.Should().Be(0);
         clone.RecNo.Should().Be(16);
         clone.SourceNo.Should().Be(7);
         clone.Keys.Count.Should().Be(0);
         clone.Items.Count.Should().Be(0);
         clone[0].Should().BeNull();
         clone["any"].Should().BeNull();
         cloneR[0].Should().BeNull();
         cloneR["any"].Should().BeNull();
         cloneU[0].Should().BeNull();
         cloneU["any"].Should().BeNull();
      }


      [TestMethod]
      public void CreateX12Segment_4Elements_CorrectSegmentCreated()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 12, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var x12Seg = rec.CreateEmptyX12Segment("NM1", 4);
         x12Seg[2] = null;
         x12Seg[4] = "blah";
 
         //assert
         x12Seg.Should().NotBeSameAs(rec);
         x12Seg.Count.Should().Be(5);  // 1 + 4
         x12Seg.RecNo.Should().Be(12);
         x12Seg.SourceNo.Should().Be(3);
         x12Seg.Keys.Count.Should().Be(5);
         x12Seg.Items.Count.Should().Be(5);

         x12Seg[0].Should().Be("NM1");
         x12Seg["Segment"].Should().Be("NM1");
         x12Seg.Keys[0].Should().Be("Segment");
         x12Seg[1].Should().Be(string.Empty);
         x12Seg["Elem001"].Should().Be(string.Empty);
         x12Seg.Keys[1].Should().Be("Elem001");
         x12Seg[2].Should().BeNull();
         x12Seg["Elem002"].Should().BeNull();
         x12Seg.Keys[2].Should().Be("Elem002");
         x12Seg[3].Should().Be(string.Empty);
         x12Seg["Elem003"].Should().Be(string.Empty);
         x12Seg.Keys[3].Should().Be("Elem003");
         x12Seg[4].Should().Be("blah");
         x12Seg["Elem004"].Should().Be("blah");
         x12Seg.Keys[4].Should().Be("Elem004");
         x12Seg[5].Should().BeNull();
         x12Seg["Elem005"].Should().BeNull();
         //x12Seg.Keys[5].Should().BeNull(); - this throws ArgumentOutOfRangeException
      }


      [TestMethod]
      public void CreateX12Segment_NoElements_CorrectSegmentCreated()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 12, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var x12Seg = rec.CreateEmptyX12Segment("LX", 0); //not really practical to have a segment with no elements

         //assert
         x12Seg.Should().NotBeSameAs(rec);
         x12Seg.Count.Should().Be(1);  // 1 + 4
         x12Seg.RecNo.Should().Be(12);
         x12Seg.SourceNo.Should().Be(3);
         x12Seg.Keys.Count.Should().Be(1);
         x12Seg.Items.Count.Should().Be(1);

         x12Seg[0].Should().Be("LX");
         x12Seg["Segment"].Should().Be("LX");
         x12Seg.Keys[0].Should().Be("Segment");
         x12Seg[1].Should().BeNull();
         x12Seg["Elem001"].Should().BeNull();
         //x12Seg.Keys[5].Should().BeNull(); - this throws ArgumentOutOfRangeException
      }


      [TestMethod]
      public void RemoveItem_SimpleValues_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 16, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         IRecord recR = rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         rec.RemoveItem("nonExisting");  //should have no effect

         //assert
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(1);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);

         //act2
         rec.RemoveItem("blah");

         //assert2
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(1);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recU[0].Should().Be("71941");
         recR[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recU[1].Should().Be("243");
         recR[1].Should().Be(243);
         Assert.AreEqual(243, recD.I_num);
         rec["blah"].Should().Be(null);
         rec[2].Should().Be(null);
         recU[2].Should().Be(null);
         recR[2].Should().Be(null);

         //act3
         rec.RemoveItem("IDCD_ID");

         //assert3
         rec.Count.Should().Be(1);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(1);
         rec.Keys[0].Should().Be("I_num");
         rec.GetItem("I_num").Key.Should().Be("I_num");
         rec[0].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recU[0].Should().Be("243");
         recR[0].Should().Be(243);
         Assert.AreEqual(243, recD.I_num);
         rec["IDCD_ID"].Should().Be(null);
         rec[1].Should().Be(null);
         recU[1].Should().Be(null);
         recR[1].Should().Be(null);
      }


      [TestMethod]
      public void AddItem_SimpleValues_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         var rec = new KeyValRecord(items, 18, 12, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         IRecord recR = rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         var result = rec.AddItem("I_alsoNum", 120);

         //assert
         result.Should().BeOfType<KeyValItem<int>>();
         result.Key.Should().Be("I_alsoNum");
         result.Value.Should().Be(120);
         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(18);
         rec.SourceNo.Should().Be(12);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);
         rec[3].Should().Be(120);
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         Assert.AreEqual(120, recD.I_alsoNum);


         //act2
         rec.AddItem("foo", "bar");

         //assert2
         rec.Count.Should().Be(5);
         rec.RecNo.Should().Be(18);
         rec.SourceNo.Should().Be(12);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);
         rec[3].Should().Be(120);
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         Assert.AreEqual(120, recD.I_alsoNum);
         rec[4].Should().Be("bar");
         recR[4].Should().Be("bar");
         recU["foo"].Should().Be("bar");
         recR[4].Should().Be("bar");
         recU["foo"].Should().Be("bar");
         Assert.AreEqual("bar", recD.foo);

         //act3
         recD.I_NUMBER = 1356;

         //assert3
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(18);
         rec.SourceNo.Should().Be(12);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);
         rec[3].Should().Be(120);
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         recR[3].Should().Be(120);
         recU["I_alsoNum"].Should().Be("120");
         Assert.AreEqual(120, recD.I_alsoNum);
         rec[4].Should().Be("bar");
         recR[4].Should().Be("bar");
         recU["foo"].Should().Be("bar");
         recR[4].Should().Be("bar");
         recU["foo"].Should().Be("bar");
         Assert.AreEqual("bar", recD.foo);
         rec[5].Should().Be(1356);
         recR[5].Should().Be(1356);
         recU["I_NUMBER"].Should().Be("1356");
         recR[5].Should().Be(1356);
         recU["I_NUMBER"].Should().Be("1356");
         Assert.AreEqual(1356, recD.I_NUMBER);
      }


      [TestMethod]
      public void AddItem_SimpleValuesButCannotAlterFields_NoChange()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         _config.AllowTransformToAlterFields = false;  //it was set to true in test Initialize method
         var rec = new KeyValRecord(items, 19, 13, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         IRecord recR = rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         var result = rec.AddItem("I_alsoNum", 120);

         //assert
         result.Should().BeNull();
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(19);
         rec.SourceNo.Should().Be(13);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);

         //act2
         result = rec.AddItem("foo", "bar");

         //assert2
         result.Should().BeNull();
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(19);
         rec.SourceNo.Should().Be(13);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);


         //act3
         recD.I_NUMBER = 1356;  //attempt to set a value of non-existing item is simply ignored(!) - this behavior may be revised

         //assert3
         Assert.IsNull(recD.I_NUMBER);
         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(19);
         rec.SourceNo.Should().Be(13);
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         recR[0].Should().Be("71941");
         recU[0].Should().Be("71941");
         Assert.AreEqual("71941", recD.IDCD_ID);
         rec[1].Should().Be("blahblah");
         rec["blah"].Should().Be("blahblah");
         Assert.AreEqual("blahblah", recD.blah);
         rec[2].Should().Be(243);
         rec["I_num"].Should().Be(243);
         recR["I_num"].Should().Be(243);
         recU["I_num"].Should().Be("243");
         Assert.AreEqual(243, recD.I_num);
      }


      [TestMethod]
      public void ReplaceItem_ExistingKey_ChangeAccepted()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("blah", "origData", _typeDefs),
                                   KeyValItem.CreateItem("I_int", 33, _typeDefs) };
         var rec = new KeyValRecord(items, 20, 10, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         rec.ReplaceItem(KeyValItem.CreateItem("blah", "66", _typeDefs));
         rec.ReplaceItem(KeyValItem.CreateItem("I_int", "badData", _typeDefs));

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(20);
         rec.SourceNo.Should().Be(10);
         rec.Keys[0].Should().Be("blah");
         rec[0].Should().Be("66");
         rec.Keys[1].Should().Be("I_int");
         rec[1].Should().Be(0);  //default(int) as a result of "badData"
      }

      [TestMethod]
      public void ReplaceItem_NonExistingKey_ChangeRejected()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("blah", "origData", _typeDefs),
                                   KeyValItem.CreateItem("I_int", 33, _typeDefs) };
         var rec = new KeyValRecord(items, 22, 11, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         rec.ReplaceItem(KeyValItem.CreateItem("I_blah", "66", _typeDefs));
         rec.ReplaceItem(KeyValItem.CreateItem("I_newInt", "badData", _typeDefs));

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(22);
         rec.SourceNo.Should().Be(11);
         rec.Keys[0].Should().Be("blah");
         rec[0].Should().Be("origData");
         rec.Keys[1].Should().Be("I_int");
         rec[1].Should().Be(33);
      }


      [TestMethod]
      public void AssignValue_TypedValueSetToStringItem_ConvertedToString()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("blah", "origData", _typeDefs),
                                   KeyValItem.CreateItem("I_int", 33, _typeDefs) };
         var rec = new KeyValRecord(items, 16, 2, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         IUntypedRecord recU = rec;
         dynamic recD = rec;

         //act
         rec["blah"] = true;

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec.Keys[0].Should().Be("blah");
         rec[0].Should().Be("True");

         //act2
         recR["blah"] = new DateTime(2050, 12, 1);

         //assert2
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec.Keys[0].Should().Be("blah");
         rec[0].Should().Be("12/1/2050 12:00:00 AM");
      }

      [TestMethod]
      public void AssignValue_BadValueSetToTypedField_DefaultData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("blah", "origData", _typeDefs),
                                   KeyValItem.CreateItem("I_int", 33, _typeDefs) };
         var rec = new KeyValRecord(items, 16, 2, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var recR = (IRecord)rec;
         dynamic recD = rec;

         //act
         rec["I_int"] = true;  //type mismatch

         //assert
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec.Keys[1].Should().Be("I_int");
         rec[1].Should().Be(0);  //default(int)

         //act2
         rec["I_int"] = 34;  //fix back to a non-default value

         //assert2
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec.Keys[1].Should().Be("I_int");
         rec[1].Should().Be(34);

         //act3
         rec["I_int"] = "35nonNum";  //bad string value

         //assert3
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(16);
         rec.SourceNo.Should().Be(2);
         rec.Keys[1].Should().Be("I_int");
         rec[1].Should().Be(0);  //default(int)
      }


      [TestMethod]
      public void GetItemClone_NoChange_SameData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };

         //act
         var rec = new KeyValRecord(items, 16, 3, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         var clone = rec.GetItemClone(items[0], items[0].Value);  //not a practical example to create exact clone of an item(!)

         //assert
         clone.Should().NotBeSameAs(items[0]);  //note that this assertion may become invalid (KeyValItem is immutable)
         clone.Should().BeOfType(typeof(KeyValItem<string>));
         clone.ItemDef.Type.Should().Be(ItemType.String);
         clone.Key.Should().Be("IDCD_ID");
         clone.Value.Should().Be("71941");
      }


      [TestMethod]
      public void GetItemClone_NewStringValue_CorrectData()
      {
         //arrange
         IItem item = KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs);
         var items = new IItem[] { item };
         int recNo = 14;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetItemClone(item, "newValue");

         //assert
         clone.Should().NotBeSameAs(item);
         clone.Should().BeOfType(typeof(KeyValItem<string>));
         clone.ItemDef.Type.Should().Be(ItemType.String);
         clone.Key.Should().Be("IDCD_ID");
         clone.Value.Should().Be("newValue");
      }


      [TestMethod]
      public void GetItemClone_NewTypedValue_CorrectData()
      {
         //arrange
         var item = KeyValItem.CreateItem("I_fld", 156, _typeDefs);
         var items = new IItem[] { item };
         int recNo = 14;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetItemClone(item, 3);

         //assert
         clone.Should().NotBeSameAs(item);
         clone.Should().BeOfType(typeof(KeyValItem<int>));
         clone.ItemDef.Type.Should().Be(ItemType.Int);
         clone.Key.Should().Be("I_fld");
         clone.Value.Should().Be(3);

         //act2
         clone = rec.GetItemClone(item, "4");

         //assert2
         clone.Should().NotBeSameAs(item);
         clone.Should().BeOfType(typeof(KeyValItem<int>));
         clone.ItemDef.Type.Should().Be(ItemType.Int);
         clone.Key.Should().Be("I_fld");
         clone.Value.Should().Be(4);
      }


      [TestMethod]
      public void GetItemClone_BadNewTypedValue_DefaultData()
      {
         //arrange
         var item = KeyValItem.CreateItem("I_fld", 156, _typeDefs);
         var items = new IItem[] { item };
         int recNo = 14;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetItemClone(item, "notNum");

         //assert
         clone.Should().NotBeSameAs(item);
         clone.Should().BeOfType(typeof(KeyValItem<int>));
         clone.ItemDef.Type.Should().Be(ItemType.Int);
         clone.Key.Should().Be("I_fld");
         clone.Value.Should().Be(0);  //default(int)
      }

   }
}
