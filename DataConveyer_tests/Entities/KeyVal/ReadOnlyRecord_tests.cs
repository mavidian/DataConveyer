//ReadOnlyRecord_tests.cs
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
using System.Linq;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class ReadOnlyRecord_tests
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
      public void RoRecCtor_SimpleValuesIgnoreMisuse_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         int recNo = 76;
         int sourceNo = 3;

         //act
         var rec = new KeyValRecord(items, recNo, sourceNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var roRec = new ReadOnlyRecord(rec);   //this ctor ignores misuse

         //assert
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(recNo);
         roRec.SourceNo.Should().Be(sourceNo);
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         roRec[1].Should().Be("blahblah");
         roRec["blah"].Should().Be("blahblah");
         roRec[2].Should().Be(243);
         roRec["I_num"].Should().Be(243);

         roRec.GetItemClone(items[0], "someNewValue").Should().BeOfType(typeof(KeyValItem<string>));

         // indexers from IRecord & IUntypedRecord interfaces:
         IRecord roRecR = roRec;
         var roRecU = (IUntypedRecord)roRec;
         roRecR[0].Should().Be("71941");
         roRecU[0].Should().Be("71941");
         roRecR["IDCD_ID"].Should().Be("71941");
         roRecU["IDCD_ID"].Should().Be("71941");
         roRecR[2].Should().Be(243);
         roRecU[2].Should().Be("243");
         roRecR["I_num"].Should().Be(243);
         roRecU["I_num"].Should().Be("243");

         roRecU.GetItemClone(items[0], "someNewValue").Should().BeOfType(typeof(KeyValItem<string>));

         // dynamic properties:
         dynamic roRecD = roRec;
         Assert.AreEqual("71941", roRecD.IDCD_ID);  //FluentAssertions don't work with dynamic properties, e.g.'string' does not contain a definition for 'Should'
         Assert.AreEqual("blahblah", roRecD.blah);
         Assert.AreEqual(243, roRecD.I_num);
         Assert.IsNull(roRecD.BadKey);

         Assert.IsInstanceOfType(roRecD.GetItemClone(items[0], "someNewValue"), typeof(KeyValItem<string>));

         var keys = roRec.Keys.ToList();
         keys.Count.Should().Be(3);
         keys[0].Should().Be("IDCD_ID");
         keys[1].Should().Be("blah");
         keys[2].Should().Be("I_num");

         var itms = roRec.Items.ToList();
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
      public void RoRecCtor_SimpleValuesThrowOnMisuse_CorrectData()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         int recNo = 77;
         int sourceNo = 2;

         //act
         var rec = new KeyValRecord(items, recNo, sourceNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var roRec = new ReadOnlyRecord(rec, true);   //this ctor throws on misuse

         //assert
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(recNo);
         roRec.SourceNo.Should().Be(sourceNo);
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec.GetItem("IDCD_ID").Key.Should().Be("IDCD_ID");
         roRec[1].Should().Be("blahblah");
         roRec["blah"].Should().Be("blahblah");
         roRec[2].Should().Be(243);
         roRec["I_num"].Should().Be(243);

         roRec.GetItemClone(items[0], "someNewValue").Should().BeOfType(typeof(KeyValItem<string>));

         // indexers from IRecord & IUntypedRecord interfaces:
         IRecord roRecR = roRec;
         var roRecU = (IUntypedRecord)roRec;
         roRecR[0].Should().Be("71941");
         roRecU[0].Should().Be("71941");
         roRecR["IDCD_ID"].Should().Be("71941");
         roRecU["IDCD_ID"].Should().Be("71941");
         roRecR[2].Should().Be(243);
         roRecU[2].Should().Be("243");
         roRecR["I_num"].Should().Be(243);
         roRecU["I_num"].Should().Be("243");

         roRecU.GetItemClone(items[0], "someNewValue").Should().BeOfType(typeof(KeyValItem<string>));

         // dynamic properties:
         dynamic roRecD = roRec;
         Assert.AreEqual("71941", roRecD.IDCD_ID);  //FluentAssertions don't work with dynamic properties, e.g.'string' does not contain a definition for 'Should'
         Assert.AreEqual("blahblah", roRecD.blah);
         Assert.AreEqual(243, roRecD.I_num);
         Assert.IsNull(roRecD.BadKey);

         Assert.IsInstanceOfType(roRecD.GetItemClone(items[0], "someNewValue"), typeof(KeyValItem<string>));

         var keys = roRec.Keys.ToList();
         keys.Count.Should().Be(3);
         keys[0].Should().Be("IDCD_ID");
         keys[1].Should().Be("blah");
         keys[2].Should().Be("I_num");

         var itms = roRec.Items.ToList();
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
      public void RoRecCtor_SimpleValuesIgnoreMisuse_UnsupportedGetsIgnored()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         int recNo = 76;
         int sourceNo = 3;

         //act
         var rec = new KeyValRecord(items, recNo, sourceNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var roRec = new ReadOnlyRecord(rec);   //this ctor ignores misuse

         //assert
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(recNo);
         roRec.SourceNo.Should().Be(sourceNo);

         //Not supported calls (misuse) - take default action (no action), do not throw:  
         roRec.RemoveItem("IDCD_ID").Should().Be(null);
         roRec.RemoveItem("NonExistingKey").Should().Be(null);
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().BeFalse();
         roRec.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().BeFalse();
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().Be(null);
         roRec.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().Be(null);
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.Count.Should().Be(3);

         roRec["IDCD_ID"] = "somethingNew";  //this is simply ignored
         roRec["NonExistingKey"] = "somethingNew";  //this is simply ignored
         roRec[0] = "somethingNew";  //this is simply ignored
         roRec[20] = "somethingNew";  //index out of range, still ignored
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.Count.Should().Be(3);
         roRec.AddItem("IDCD_ID", "1000").Should().BeNull();
         roRec.AddItem("NonExistingKey", "1000").Should().BeNull();
         roRec.Count.Should().Be(3);
         roRec.GetClone().Should().BeNull();
         roRec.GetEmptyClone().Should().BeNull();

         // ReadOnlyRecord as IRecord:
         IRecord roRecR = roRec;
         roRecR.RemoveItem("IDCD_ID").Should().Be(null);
         roRecR.RemoveItem("NonExistingKey").Should().Be(null);
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().BeFalse();
         roRecR.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().BeFalse();
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().Be(null);
         roRecR.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().Be(null);
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.Count.Should().Be(3);

         roRecR["IDCD_ID"] = "somethingNew";  //this is simply ignored
         roRecR["NonExistingKey"] = "somethingNew";  //this is simply ignored
         roRecR[0] = "somethingNew";  //this is simply ignored
         roRecR[20] = "somethingNew";  //index out of range, still ignored
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.Count.Should().Be(3);
         roRecR.AddItem("IDCD_ID", "1000").Should().BeNull();
         roRecR.AddItem("NonExistingKey", "1000").Should().BeNull();
         roRecR.Count.Should().Be(3);
         roRecR.GetClone().Should().BeNull();
         roRecR.GetEmptyClone().Should().BeNull();


         // ReadOnlyRecord as IUntypedRecord:
         var roRecU = (IUntypedRecord)roRec;
         roRecU.RemoveItem("IDCD_ID").Should().Be(null);
         roRecU.RemoveItem("NonExistingKey").Should().Be(null);
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().BeFalse();
         roRecU.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().BeFalse();
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)).Should().Be(null);
         roRecU.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)).Should().Be(null);
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.Count.Should().Be(3);

         roRecU["IDCD_ID"] = "somethingNew";  //this is simply ignored
         roRecU["NonExistingKey"] = "somethingNew";  //this is simply ignored
         roRecU[0] = "somethingNew";  //this is simply ignored
         roRecU[20] = "somethingNew";  //index out of range, still ignored
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.Count.Should().Be(3);
         roRecU.AddItem("IDCD_ID", "1000").Should().BeNull();
         roRecU.AddItem("NonExistingKey", "1000").Should().BeNull();
         roRecU.Count.Should().Be(3);
         roRecU.GetClone().Should().BeNull();
         roRecU.GetEmptyClone().Should().BeNull();

         // dynamic properties:
         dynamic roRecD = roRec;
         roRecD.IDCD_ID = "somethingNew";  //this is simply ignored
         Assert.AreEqual("71941", roRecD.IDCD_ID);  //FluentAssertions don't work with dynamic properties, e.g.'string' does not contain a definition for 'Should'
         roRecD.blah = "somethingNew";
         Assert.AreEqual("blahblah", roRecD.blah);
         roRecD.I_num = 5;
         Assert.AreEqual(243, roRecD.I_num);
         roRecD.NonExisting = "something";
         Assert.IsNull(roRecD.NonExisting);
         Assert.AreEqual(3, roRecD.Count);
      }

      [TestMethod]
      public void RoRecCtor_SimpleValuesThrowOnMisuse_UnsupportedThrows()
      {
         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("IDCD_ID", "71941", _typeDefs),
                                   KeyValItem.CreateItem("blah", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("I_num", 243, _typeDefs) };
         int recNo = 76;
         int sourceNo = 3;

         //act
         var rec = new KeyValRecord(items, recNo, sourceNo, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);
         var roRec = new ReadOnlyRecord(rec, true);   //this ctor causes misuse to throw NotSupportedException

         //assert
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(recNo);
         roRec.SourceNo.Should().Be(sourceNo);

         //Not supported calls (misuse) throw NotSupportedException:
         Action a = () => { roRec.RemoveItem("IDCD_ID"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec.RemoveItem("NonExistingKey"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         a = () => { roRec.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         a = () => { roRec.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.Count.Should().Be(3);

         a = () => { roRec["IDCD_ID"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec["NonExistingKey"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec[0] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec[20] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec.Count.Should().Be(3);
         a = () => { roRec.AddItem("IDCD_ID", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec.AddItem("NonExistingKey", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Count.Should().Be(3);
         a = () => { roRec.GetClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRec.GetEmptyClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");

         // ReadOnlyRecord as IRecord:
         IRecord roRecR = roRec;
         a = () => { roRecR.RemoveItem("IDCD_ID"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR.RemoveItem("NonExistingKey"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         a = () => { roRecR.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         a = () => { roRecR.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.Count.Should().Be(3);

         a = () => { roRecR["IDCD_ID"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR["NonExistingKey"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR[0] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR[20] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecR.Keys[0].Should().Be("IDCD_ID");
         roRecR[0].Should().Be("71941");
         roRecR.Count.Should().Be(3);

         a = () => { roRecR.AddItem("IDCD_ID", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR.AddItem("NonExistingKey", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecR.Count.Should().Be(3);
         a = () => { roRecR.GetClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecR.GetEmptyClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");


         // ReadOnlyRecord as IUntypedRecord:
         var roRecU = (IUntypedRecord)roRec;
         a = () => { roRecU.RemoveItem("IDCD_ID"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU.RemoveItem("NonExistingKey"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         a = () => { roRecU.ReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU.ReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         a = () => { roRecU.AddOrReplaceItem(KeyValItem.CreateItem("IDCD_ID", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU.AddOrReplaceItem(KeyValItem.CreateItem("NonExistingKey", "1000", _typeDefs)); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.Count.Should().Be(3);

         a = () => { roRecU["IDCD_ID"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU["NonExistingKey"] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU[0] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU[20] = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecU.Keys[0].Should().Be("IDCD_ID");
         roRecU[0].Should().Be("71941");
         roRecU.Count.Should().Be(3);
         a = () => { roRecU.AddItem("IDCD_ID", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU.AddItem("NonExistingKey", "1000"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRecU.Count.Should().Be(3);
         a = () => { roRecU.GetClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         a = () => { roRecU.GetEmptyClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");

         // dynamic properties:
         dynamic roRecD = roRec;
         a = () => { roRecD.IDCD_ID = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         Assert.AreEqual("71941", roRecD.IDCD_ID);  //FluentAssertions don't work with dynamic properties, e.g.'string' does not contain a definition for 'Should'
         a = () => { roRecD.blah = "somethingNew"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         Assert.AreEqual("blahblah", roRecD.blah);
         a = () => { roRecD.I_num = 5; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         //Assert.AreEqual(243, roRecD.I_num);
         a = () => { roRecD.NonExisting = "something"; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         Assert.IsNull(roRecD.NonExisting);
         Assert.AreEqual(3, roRecD.Count);
      }


   }
}
