//IUntypedRecord_tests.cs
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
   public class IUntypedRecord_tests
   {
      private OrchestratorConfig _config;

      private TypeDefinitions _typeDefs;
      private IItem[] _items;
      private int _recNo;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();
         _config.AllowTransformToAlterFields = true;
         _config.PropertyBinEntities = PropertyBinAttachedTo.Records;

         // simple type definitions, everything string, except for fields starting with I_ (int) & D_ (DateTime
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("I_") ? ItemType.Int : key.StartsWith("D_") ? ItemType.DateTime : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();
         _typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);
         _items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                KeyValItem.CreateItem("D_date", new DateTime(2012,12,28), _typeDefs),
                                KeyValItem.CreateItem("I_num", 184, _typeDefs) };
         _recNo = 26;
      }

      [TestMethod]
      public void IndexersEtc_SimpleValues_CorrectData()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act

         //assert
         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         // indexers (all string values)
         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[1].Should().BeOfType(typeof(string));
         rec[1].Should().Be("blahblah");
         rec[2].Should().BeOfType(typeof(string));
         rec[2].Should().Be("12/28/2012 12:00:00 AM");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("184");
         rec[4].Should().BeNull();
         rec["IDCD_ID"].Should().BeSameAs(rec[0]);
         rec["blah"].Should().BeSameAs(rec[1]);
         rec["D_date"].Should().Be(rec[2]);  //note that DateTime is value type, so that indexer returns boxed value (cannot use BeSameAs)
         rec["I_num"].Should().Be(rec[3]);
         rec["badKey"].Should().BeNull();

         // Keys
         rec.Keys[0].Should().Be("IDCD_ID");
         rec.Keys[1].Should().Be("blah");
         rec.Keys[2].Should().Be("D_date");
         rec.Keys[3].Should().Be("I_num");
         Action a = () => { var x = rec.Keys[4]; };  //attempt to access non-existent key
         a.Should().Throw<ArgumentOutOfRangeException>();

         // Items (their values are typed)
         rec.Items[0].Key.Should().Be("IDCD_ID");
         rec.Items[0].Value.Should().Be("71941");
         rec.Items[0].StringValue.Should().Be("71941");
         rec.Items[0].ItemDef.Type.Should().Be(ItemType.String);
         rec.Items[0].ItemDef.Format.Should().Be(String.Empty);
         rec.Items[1].Value.Should().Be("blahblah");
         rec.Items[2].Value.Should().Be(DateTime.Parse("12/28/2012"));
         rec.Items[2].StringValue.Should().Be("12/28/2012 12:00:00 AM");
         rec.Items[2].ItemDef.Type.Should().Be(ItemType.DateTime);
         rec.Items[3].Value.Should().Be(184);  //note int type
         rec.Items[3].StringValue.Should().Be("184");
         rec.Items[3].ItemDef.Type.Should().Be(ItemType.Int);
         a = () => { var x = rec.Items[4]; };  //attempt to access non-existent item
         a.Should().Throw<ArgumentOutOfRangeException>();

         // ContainsKey
         rec.ContainsKey("IDCD_ID").Should().BeTrue();
         rec.ContainsKey("71941").Should().BeFalse();
         rec.ContainsKey("I_num").Should().BeTrue();
         rec.ContainsKey("badKey").Should().BeFalse();

         // GetItem
         rec.GetItem(0).Should().BeSameAs(rec.Items[0]);
         rec.GetItem(0).Should().BeSameAs(rec.GetItem("IDCD_ID"));
         rec.GetItem(1).Should().BeSameAs(rec.Items[1]);
         rec.GetItem(1).Should().BeSameAs(rec.GetItem("blah"));
         rec.GetItem(2).Should().BeSameAs(rec.Items[2]);
         rec.GetItem(2).Should().BeSameAs(rec.GetItem("D_date"));
         rec.GetItem(3).Should().BeSameAs(rec.Items[3]);
         rec.GetItem(3).Should().BeSameAs(rec.GetItem("I_num"));
         rec.GetItem(4).Should().BeOfType(typeof(VoidKeyValItem));
         rec.GetItem("badKey").Should().BeOfType(typeof(VoidKeyValItem));
      }


      [TestMethod]
      public void IndexerSet_SimpleValues_ValuesAssigned()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         rec[0] = "new719..";
         rec[3] = "1000"; //need to pass string (parsable to int, i.e. w/o a fraction)
         rec["D_date"] = "1/1/1753";  //OK to assign parsable string

         //assert
         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("new719..");
         rec[1].Should().BeOfType(typeof(string));
         rec[1].Should().Be("blahblah");
         rec[2].Should().BeOfType(typeof(string));
         rec[2].Should().Be("1/1/1753 12:00:00 AM");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("1000");
      }

      [TestMethod]
      public void IndexerSet_BadValues_DefaultValuesAssigned()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         rec[3] = "1000.";  //this string is not parsable to int
         rec["D_date"] = "notDate";

         //assert
         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[1].Should().BeOfType(typeof(string));
         rec[1].Should().Be("blahblah");
         rec[2].Should().BeOfType(typeof(string));
         rec[2].Should().Be(default(DateTime).ToString());
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("0");
      }


      [TestMethod]
      public void RemoveItem_ExistingKey_RemoveItemAndReturnTrue()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var result = rec.RemoveItem("D_date");

         //assert
         result.Should().BeTrue();

         rec.Count.Should().Be(3);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[1].Should().BeOfType(typeof(string));
         rec[1].Should().Be("blahblah");
         rec[2].Should().BeOfType(typeof(string));
         rec[2].Should().Be("184");
      }


      [TestMethod]
      public void RemoveItem_NonExistingKey_ReturnFalse()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var result = rec.RemoveItem("badKey");

         //assert
         result.Should().BeFalse();

         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("184");
      }


      [TestMethod]
      public void AddItem_SampleValue_ReturnAddedItem()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var result = rec.AddItem("I_someNum", "360");

         //assert
         result.Should().BeOfType(typeof(KeyValItem<int>));
         result.Key.Should().Be("I_someNum");
         result.Value.Should().Be(360);
         result.ItemDef.Type.Should().Be(ItemType.Int);

         rec.Count.Should().Be(5);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("184");
         rec[4].Should().BeOfType(typeof(string));
         rec[4].Should().Be("360");
         rec.Keys[4].Should().Be("I_someNum");
      }


      [TestMethod]
      public void AddItem_ExistingKey_ReturnVoidItem()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var result = rec.AddItem("blah", "someOtherBlah");

         //assert
         result.Should().BeOfType(typeof(VoidKeyValItem));
         result.Value.Should().BeNull();

         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("184");
      }


      [TestMethod]
      public void AddOrReplaceItem_NewKey_AddAndReturnFalse()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var itemToAdd = KeyValItem.CreateItem("I_newNum", 518, _typeDefs);

         //act
         var result = rec.AddOrReplaceItem(itemToAdd);

         //assert
         result.Should().BeFalse();

         rec.Count.Should().Be(5);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("184");
         rec[4].Should().BeOfType(typeof(string));
         rec[4].Should().Be("518");
         rec.Keys[4].Should().Be("I_newNum");
      }


      [TestMethod]
      public void AddOrReplaceItem_ExistingKey_ReplaceAndReturnsTrue()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var itemToReplace = KeyValItem.CreateItem("I_num", 518, _typeDefs);

         //act
         var result = rec.AddOrReplaceItem(itemToReplace);

         //assert
         result.Should().BeTrue();

         rec.Count.Should().Be(4);
         rec.RecNo.Should().Be(26);

         rec[0].Should().BeOfType(typeof(string));
         rec[0].Should().Be("71941");
         rec[3].Should().BeOfType(typeof(string));
         rec[3].Should().Be("518");
         rec.Keys[3].Should().Be("I_num");
      }


      [TestMethod]
      public void GetClone_Bins_SameInstances()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, new Dictionary<string, object>(), new Dictionary<string, object>(), _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetClone();

         //assert
         clone.TraceBin.Should().NotBeNull();
         clone.TraceBin.Should().BeSameAs(rec.TraceBin);
         clone.PropertyBin.Should().NotBeNull();
         clone.PropertyBin.Should().BeSameAs(rec.PropertyBin);
      }


      [TestMethod]
      public void GetEmptyClone_Bins_SameInstances()
      {
         //arrange
         IUntypedRecord rec = new KeyValRecord(_items, _recNo, 1, 0, null, new Dictionary<string, object>(), new Dictionary<string, object>(), _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //act
         var clone = rec.GetEmptyClone();

         //assert
         clone.TraceBin.Should().NotBeNull();
         clone.TraceBin.Should().BeSameAs(rec.TraceBin);
         clone.PropertyBin.Should().NotBeNull();
         clone.PropertyBin.Should().BeSameAs(rec.PropertyBin);
      }
   }
}
