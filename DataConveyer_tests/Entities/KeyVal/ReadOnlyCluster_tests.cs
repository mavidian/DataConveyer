//ReadOnlyCluster_tests.cs
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

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class ReadOnlyCluster_tests
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
      public void RoClstrCtor_SimpleValuesIgnoreMisuse_CorrectData()
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
         var roClstr = new ReadOnlyCluster(clstr);  // this ctor causes objReadOnlyCluster to ignore misuse

         //assert
         roClstr.Count.Should().Be(2);
         roClstr.ClstrNo.Should().Be(9);
         roClstr.StartRecNo.Should().Be(16);
         roClstr.StartSourceNo.Should().Be(78);

         roClstr.Records.Count.Should().Be(2);

         var roRec = roClstr[0];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(16);
         roRec.SourceNo.Should().Be(7);
         roRec.Items[0].Key.Should().Be("IDCD_ID");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec[1].Should().Be("blahblah");
         roRec["blah"].Should().Be("blahblah");
         roRec[2].Should().Be(243);
         roRec["I_num"].Should().Be(243);
         roClstr.GetRecord(0).Should().Be(roRec);

         roRec = roClstr[1];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.Count.Should().Be(2);
         roRec.RecNo.Should().Be(17);
         roRec.SourceNo.Should().Be(8);
         roRec.Keys[0].Should().Be("I_#");
         roRec[0].Should().Be(15);
         roRec["I_#"].Should().Be(15);
         roRec.Keys[1].Should().Be("Fld1");
         roRec["Fld1"].Should().Be("data1");
         roClstr.GetRecord(1).Should().Be(roRec);
      }


      [TestMethod]
      public void RoClstrCtor_SimpleValuesThrowOnMisuse_CorrectData()
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
         var roClstr = new ReadOnlyCluster(clstr, true);  // this ctor causes objReadOnlyCluster to throw NotSupportedexception on misuse

         //assert
         roClstr.Count.Should().Be(2);
         roClstr.ClstrNo.Should().Be(9);
         roClstr.StartRecNo.Should().Be(16);
         roClstr.StartSourceNo.Should().Be(78);

         roClstr.Records.Count.Should().Be(2);

         var roRec = roClstr[0];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.Count.Should().Be(3);
         roRec.RecNo.Should().Be(16);
         roRec.SourceNo.Should().Be(7);
         roRec.Items[0].Key.Should().Be("IDCD_ID");
         roRec.Keys[0].Should().Be("IDCD_ID");
         roRec[0].Should().Be("71941");
         roRec[1].Should().Be("blahblah");
         roRec["blah"].Should().Be("blahblah");
         roRec[2].Should().Be(243);
         roRec["I_num"].Should().Be(243);
         roClstr.GetRecord(0).Should().Be(roRec);

         roRec = roClstr[1];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.Count.Should().Be(2);
         roRec.RecNo.Should().Be(17);
         roRec.SourceNo.Should().Be(8);
         roRec.Keys[0].Should().Be("I_#");
         roRec[0].Should().Be(15);
         roRec["I_#"].Should().Be(15);
         roRec.Keys[1].Should().Be("Fld1");
         roRec["Fld1"].Should().Be("data1");
         roClstr.GetRecord(1).Should().Be(roRec);
      }


      [TestMethod]
      public void RoClstrCtor_SimpleValuesIgnoreMisuse_UnsupportedGetsIgnored()
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
         var roClstr = new ReadOnlyCluster(clstr);  // this ctor causes objReadOnlyCluster to ignore misuse

         //assert
         roClstr.Count.Should().Be(2);
         roClstr.ClstrNo.Should().Be(9);
         roClstr.StartRecNo.Should().Be(16);
         roClstr.StartSourceNo.Should().Be(78);

         roClstr.Records.Count.Should().Be(2);

         roClstr[0] = null;
         roClstr[0].Should().NotBeNull();
         roClstr.RemoveRecord(0).Should().BeFalse();
         roClstr.Count.Should().Be(2);
         roClstr.Records.Count.Should().Be(2);
         roClstr.AddRecord(null).Should().BeFalse();
         roClstr.Count.Should().Be(2);
         roClstr.Records.Count.Should().Be(2);
         roClstr.GetClone().Should().BeNull();
         roClstr.GetEmptyClone().Should().BeNull();

         var roRec = roClstr[0];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.RemoveItem("IDCD_ID").Should().Be(null);
         roRec.Count.Should().Be(3);

         roRec = roClstr[1];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         roRec.AddItem("a", "b").Should().BeNull();
         roRec.Count.Should().Be(2);
      }


      [TestMethod]
      public void RoClstrCtor_SimpleValuesThrowOnMisuse_UnsupportedThrows()
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
         var roClstr = new ReadOnlyCluster(clstr, true);  //this ctor causes objReadOnlyCluster to throw NotSupportedexception on misuse

         //assert
         roClstr.Count.Should().Be(2);
         roClstr.ClstrNo.Should().Be(9);
         roClstr.StartRecNo.Should().Be(16);
         roClstr.StartSourceNo.Should().Be(78);

         roClstr.Records.Count.Should().Be(2);

         Action a = () => { roClstr[0] = null; };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyCluster object.");
         roClstr[0].Should().NotBeNull();
         a = () => { roClstr.RemoveRecord(0); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyCluster object.");
         roClstr.Count.Should().Be(2);
         roClstr.Records.Count.Should().Be(2);
         a = () => { roClstr.AddRecord(null); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyCluster object.");
         roClstr.Count.Should().Be(2);
         roClstr.Records.Count.Should().Be(2);
         a = () => { roClstr.GetClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyCluster object.");
         a = () => { roClstr.GetEmptyClone(); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyCluster object.");

         var roRec = roClstr[0];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         a = () => { roRec.RemoveItem("IDCD_ID"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Count.Should().Be(3);

         roRec = roClstr[1];
         roRec.Should().BeOfType<ReadOnlyRecord>();
         a = () => { roRec.AddItem("a", "b"); };
         a.Should().Throw<NotSupportedException>().WithMessage("Unsupported operation invoked on ReadOnlyRecord object.");
         roRec.Count.Should().Be(2);
      }

   }
}
