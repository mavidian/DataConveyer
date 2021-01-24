//KeyValItem_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class KeyValItem_tests
   {
      [TestMethod]
      public void CreateItem_StringValue_CorrectData()
      {
         //arrange
         // simple type definitions, everything string
         Func<string, ItemType> fldTypeFunc = key => ItemType.String; ;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //act
         var item = KeyValItem.CreateItem("IDCD_ID", "71941", typeDefs);
         //item is of IItem type

         //assert
         item.Should().BeOfType(typeof(KeyValItem<string>));
         item.ItemDef.Type.Should().Be(ItemType.String);
         item.Key.Should().Be("IDCD_ID");
         item.Value.Should().Be("71941");
         item.StringValue.Should().Be("71941");
      }

      [TestMethod]
      public void CreateItem_StringNullValue_CorrectData()
      {
         //arrange
         // simple type definitions, everything string
         Func<string, ItemType> fldTypeFunc = key => ItemType.String; ;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //act
         var item = KeyValItem.CreateItem("IDCD_ID", null, typeDefs);
         //item is of IItem type

         //assert
         item.Should().BeOfType(typeof(KeyValItem<string>));
         item.ItemDef.Type.Should().Be(ItemType.String);
         item.Key.Should().Be("IDCD_ID");
         item.Value.Should().BeNull();
         item.StringValue.Should().BeNull();
      }


      [TestMethod]
      public void CreateItem_TypedValues_CorrectData()
      {
         //arrange
         // 3 fields of specific types, everything else string
         Func<string, ItemType> fldTypeFunc = key => ItemType.String; ;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>()
                                                           {
                                                              { "D_fld", ItemType.DateTime },
                                                              { "M_fld", ItemType.Decimal },
                                                              { "I_fld", ItemType.Int }
                                                           });
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>(new Dictionary<string, string>()
                                                           {
                                                              { "D_fld", "yyyyMMdd" },
                                                              { "M_fld", "$##,##0.00" },
                                                              { "I_fld", "000,000" }
                                                           });
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);


         //act (string)
         var item = KeyValItem.CreateItem("IDCD_ID", "71941", typeDefs);
         var itemS = (ITypedItem<string>)item;
         //item is of IItem type, itemS is of ITypedItem<string> type

         //assert (string)
         item.Should().BeOfType(typeof(KeyValItem<string>));
         item.ItemDef.Type.Should().Be(ItemType.String);
         item.Key.Should().Be("IDCD_ID");
         item.Value.Should().Be("71941");
         item.StringValue.Should().Be("71941");
         //ITypedItem<string>
         itemS.Should().BeOfType(typeof(KeyValItem<string>));
         itemS.ItemDef.Type.Should().Be(ItemType.String);
         itemS.Key.Should().Be("IDCD_ID");
         itemS.Value.Should().Be("71941");
         itemS.StringValue.Should().Be("71941");


         //act (DateTime)
         var myDate = new DateTime(2009, 3, 31);
         item = KeyValItem.CreateItem("D_fld", myDate, typeDefs);
         var itemD = (ITypedItem<DateTime>)item;
         //item is of IItem type, itemD is of ITypedItem<DateTime> type

         //assert (DateTime)
         item.Should().BeOfType(typeof(KeyValItem<DateTime>));
         item.ItemDef.Type.Should().Be(ItemType.DateTime);
         item.Key.Should().Be("D_fld");
         item.Value.Should().Be(myDate);
         item.StringValue.Should().Be("20090331");
         //ITypedItem<DateTime>
         itemD.Should().BeOfType(typeof(KeyValItem<DateTime>));
         itemD.ItemDef.Type.Should().Be(ItemType.DateTime);
         itemD.Key.Should().Be("D_fld");
         itemD.Value.Should().Be(myDate);
         itemD.StringValue.Should().Be("20090331");


         //act (Decimal)
         item = KeyValItem.CreateItem("M_fld", .26m, typeDefs);
         var itemM = (ITypedItem<Decimal>)item;
         //item is of IItem type, itemM is of ITypedItem<Decimal> type

         //assert (Decimal)
         item.Should().BeOfType(typeof(KeyValItem<Decimal>));
         item.ItemDef.Type.Should().Be(ItemType.Decimal);
         item.Key.Should().Be("M_fld");
         item.Value.Should().Be(.26m);
         item.StringValue.Should().Be("$0.26");
         //ITypedItem<Decimal>
         itemM.Should().BeOfType(typeof(KeyValItem<Decimal>));
         itemM.ItemDef.Type.Should().Be(ItemType.Decimal);
         itemM.Key.Should().Be("M_fld");
         itemM.Value.Should().Be(.26m);
         itemM.StringValue.Should().Be("$0.26");


         //act (int)
         item = KeyValItem.CreateItem("I_fld", 93, typeDefs);
         var itemI = (ITypedItem<int>)item;
         //item is of IItem type, itemI is of ITypedItem<int> type

         //assert (int)
         item.Should().BeOfType(typeof(KeyValItem<int>));
         item.ItemDef.Type.Should().Be(ItemType.Int);
         item.Key.Should().Be("I_fld");
         item.Value.Should().Be(93);
         item.StringValue.Should().Be("000,093");
         //ITypedItem<int>
         itemI.Should().BeOfType(typeof(KeyValItem<int>));
         itemI.ItemDef.Type.Should().Be(ItemType.Int);
         itemI.Key.Should().Be("I_fld");
         itemI.Value.Should().Be(93);
         itemI.StringValue.Should().Be("000,093");

         //act (int assigned as string)
         item = KeyValItem.CreateItem("I_fld", "94", typeDefs);
         itemI = (ITypedItem<int>)item;
         //item is of IItem type, itemI is of ITypedItem<int> type

         //assert (int)
         item.Should().BeOfType(typeof(KeyValItem<int>));
         item.ItemDef.Type.Should().Be(ItemType.Int);
         item.Key.Should().Be("I_fld");
         item.Value.Should().Be(94);
         item.StringValue.Should().Be("000,094");
         //ITypedItem<int>
         itemI.Should().BeOfType(typeof(KeyValItem<int>));
         itemI.ItemDef.Type.Should().Be(ItemType.Int);
         itemI.Key.Should().Be("I_fld");
         item.Value.Should().Be(94);
         itemI.StringValue.Should().Be("000,094");
      }


      [TestMethod]
      public void CreateItem_TypedDefaultValues_CorrectData()
      {
         //arrange
         // 3 fields of specific types, everything else string
         Func<string, ItemType> fldTypeFunc = key => ItemType.String; ;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>()
                                                           {
                                                              { "D_fld", ItemType.DateTime },
                                                              { "M_fld", ItemType.Decimal },
                                                              { "I_fld", ItemType.Int }
                                                           });
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>(new Dictionary<string, string>()
                                                           {
                                                              { "D_fld", "yyyyMMdd" },
                                                              { "M_fld", "$##,##0.00" },
                                                              { "I_fld", "000,000" }
                                                           });
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //passing a null value should result in the default value for the given type

         //act (string)
         var item = KeyValItem.CreateItem("IDCD_ID", null, typeDefs);
         var itemS = (ITypedItem<string>)item;
         //item is of IItem type, itemS is of ITypedItem<string> type

         //assert (string)
         item.Should().BeOfType(typeof(KeyValItem<string>));
         item.ItemDef.Type.Should().Be(ItemType.String);
         item.Key.Should().Be("IDCD_ID");
         item.Value.Should().BeNull();
         item.StringValue.Should().BeNull();
         //ITypedItem<string>
         itemS.Should().BeOfType(typeof(KeyValItem<string>));
         itemS.ItemDef.Type.Should().Be(ItemType.String);
         itemS.Key.Should().Be("IDCD_ID");
         itemS.Value.Should().BeNull();
         itemS.StringValue.Should().BeNull();


         //act (DateTime)
         item = KeyValItem.CreateItem("D_fld", null, typeDefs);
         var itemD = (ITypedItem<DateTime>)item;
         //item is of IItem type, itemD is of ITypedItem<DateTime> type

         //assert (DateTime)
         item.Should().BeOfType(typeof(KeyValItem<DateTime>));
         item.ItemDef.Type.Should().Be(ItemType.DateTime);
         item.Key.Should().Be("D_fld");
         item.Value.Should().Be(DateTime.MinValue);  //same as DateTime.MinValue
         item.StringValue.Should().Be("00010101");
         //ITypedItem<DateTime>
         itemD.Should().BeOfType(typeof(KeyValItem<DateTime>));
         itemD.ItemDef.Type.Should().Be(ItemType.DateTime);
         itemD.Key.Should().Be("D_fld");
         itemD.Value.Should().Be(default(DateTime));
         itemD.StringValue.Should().Be("00010101");


         //act (Decimal)
         item = KeyValItem.CreateItem("M_fld", null, typeDefs);
         var itemM = (ITypedItem<Decimal>)item;
         //item is of IItem type, itemM is of ITypedItem<Decimal> type

         //assert (Decimal)
         item.Should().BeOfType(typeof(KeyValItem<Decimal>));
         item.ItemDef.Type.Should().Be(ItemType.Decimal);
         item.Key.Should().Be("M_fld");
         item.Value.Should().Be(default(Decimal));
         item.StringValue.Should().Be("$0.00");
         //ITypedItem<Decimal>
         itemM.Should().BeOfType(typeof(KeyValItem<Decimal>));
         itemM.ItemDef.Type.Should().Be(ItemType.Decimal);
         itemM.Key.Should().Be("M_fld");
         itemM.Value.Should().Be(default(Decimal));
         itemM.StringValue.Should().Be("$0.00");


         //act (int)
         item = KeyValItem.CreateItem("I_fld", null, typeDefs);
         var itemI = (ITypedItem<int>)item;
         //item is of IItem type, itemI is of ITypedItem<int> type

         //assert (int)
         item.Should().BeOfType(typeof(KeyValItem<int>));
         item.ItemDef.Type.Should().Be(ItemType.Int);
         item.Key.Should().Be("I_fld");
         item.Value.Should().Be(default(int));
         item.StringValue.Should().Be("000,000");
         //ITypedItem<int>
         itemI.Should().BeOfType(typeof(KeyValItem<int>));
         itemI.ItemDef.Type.Should().Be(ItemType.Int);
         itemI.Key.Should().Be("I_fld");
         itemI.Value.Should().Be(default(int));
         itemI.StringValue.Should().Be("000,000");
      }

   }
}