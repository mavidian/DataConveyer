//TypeDefinitions_tests.cs
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
using FluentAssertions.Extensions;
using Mavidian.DataConveyer.Entities.KeyVal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace DataConveyer_tests.Entities.KeyVal
{
   [TestClass]
   public class TypeDefinitions_tests
   {
      [TestMethod]
      public void Ctor_AllDefaults_CorrectData()
      {
         //arrange
         //  minimal, default settings:
         Func<string, ItemType> fldTypeFunc = key => ItemType.String; ;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>();
         Func<string, string> fldFormatFunc = key => string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>();

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //assert
         typeDefs.GetFldType("f1").Should().Be(ItemType.String);
         typeDefs.GetFldFormat("f1").Should().Be(string.Empty);
         typeDefs.GetFldParser("f1").Should().BeOfType(typeof(Func<string, object>));
         typeDefs.GetFldParser("f1")("anything").Should().BeOfType(typeof(String));
         typeDefs.GetFldParser("f1")(" anything, really ! ").Should().Be(" anything, really ! ");

         typeDefs.GetFldFormat("f2").Should().Be(string.Empty);
         typeDefs.GetFldParser("f3")(string.Empty).Should().Be(string.Empty);

         typeDefs.GetFldFormat("f2").Should().Be(string.Empty);  //this BTW, is retrieved from memoizing cache
      }


      [TestMethod]
      public void Ctor_SampleDefinions_CorrectData()
      {
         //arrange
         // somewhat typical settings (starting w/M_ = money, anything else Integer, plus some exceptions in initial values):
         // note that TypeDefinitions does not require types and formats to be setup for identical set of keys like below,
         //  (but this is how the data gets lined up by the EtlOrchestrator)
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("M_") ? ItemType.Decimal : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>()
                                                           {
                                                              { "DateFld", ItemType.DateTime },
                                                              { "M_alsoDate", ItemType.DateTime },
                                                              { "BoolFld", ItemType.Bool },
                                                              { "IntFld", ItemType.Int }
                                                           });
         Func<string, string> fldFormatFunc = key => key.StartsWith("M_") ? "$##0.00" : string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>(new Dictionary<string, string>()
                                                           {
                                                              { "DateFld", "yyyyMMdd" },
                                                              { "M_alsoDate", "yymmdd" },
                                                              { "IntFld", "000,000" }
                                                           });

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //assert
         typeDefs.GetFldType("undef").Should().Be(ItemType.String);
         typeDefs.GetFldFormat("undef").Should().Be(string.Empty);
         typeDefs.GetFldParser("IntFld")("nonInt").Should().BeOfType(typeof(int));
         typeDefs.GetFldParser("IntFld")("nonInt").Should().Be(default(int));

         typeDefs.GetFldType("M_mny").Should().Be(ItemType.Decimal);
         typeDefs.GetFldFormat("M_mny").Should().Be("$##0.00");
         typeDefs.GetFldParser("M_mny")(" 0012.55 ").Should().BeOfType<Decimal>();
         typeDefs.GetFldParser("M_mny")(" 0012.55 ").Should().Be(12.55m);

         typeDefs.GetFldParser("M_alsoDate")("1-JAN-2010").Should().BeOfType<DateTime>();
         typeDefs.GetFldParser("M_alsoDate")("1-JAN-2010").Should().Be(DateTime.Parse("1/1/2010"));
         typeDefs.GetFldFormat("M_alsoDate").Should().Be("yymmdd");
         typeDefs.GetFldType("M_alsoDate").Should().Be(ItemType.DateTime);

         typeDefs.GetFldFormat("IntFld").Should().Be("000,000");
         typeDefs.GetFldParser("IntFld")("  -01932 ").Should().BeOfType<int>();
         typeDefs.GetFldParser("IntFld")("  -01932 ").Should().Be(-1932);
         typeDefs.GetFldType("IntFld").Should().Be(ItemType.Int);

         typeDefs.GetFldType("DateFld").Should().Be(ItemType.DateTime);
         typeDefs.GetFldFormat("DateFld").Should().Be("yyyyMMdd");

         typeDefs.GetFldType("BoolFld").Should().Be(ItemType.Bool);
         typeDefs.GetFldFormat("BoolFld").Should().Be(string.Empty);
         typeDefs.GetFldParser("BoolFld")("any").Should().BeOfType<bool>();
         typeDefs.GetFldParser("BoolFld")("any").Should().Be(false);
         typeDefs.GetFldParser("BoolFld")("yes").Should().Be(false);  //note that anything but true (or TRUE) will result in false - this is how bool.TryParse works
         typeDefs.GetFldParser("BoolFld")("true").Should().Be(true);
      }


      [TestMethod]
      public void GetMethods_CalledMultipleTimes_AreIndeedMemoized()
      {
         //arrange
         //  default settings, but methods take long time (5ms delay):
         Func<string, ItemType> fldTypeFunc = key => { Thread.Sleep(5); return ItemType.String; };
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>() { { "F2", ItemType.Int } });
         Func<string, string> fldFormatFunc = key => { Thread.Sleep(5); return string.Empty; };
         var initFldFormats = new ConcurrentDictionary<string, string>();

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);
         Action getFT1 = () => typeDefs.GetFldType("F1");
         Action getFT2 = () => typeDefs.GetFldType("F2");
         Action getFT4 = () => typeDefs.GetFldType("F4");
         Action getFF1 = () => typeDefs.GetFldFormat("F1");
         Action getFF3 = () => typeDefs.GetFldFormat("F3");
         Action getFP1 = () => typeDefs.GetFldParser("F1");              //this action doesn't execute the returned function, hence no dependent GetFldType (getFT1 action) is called
         Action getFP4 = () => typeDefs.GetFldParser("F4")("anything");  //this actually consumes the function, which causes dependent GetFldType function (getFT4 action) to be called and hence memoized


         //assert
         getFT1.ExecutionTime().Should().BeGreaterThan(5.Milliseconds());  //first time not memoized; this executes for 5+ms
         getFT1.ExecutionTime().Should().BeLessThan(4.Milliseconds());     //second time memoized

         getFF1();   //this executes for 5+ms
         getFF1.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         //here, no need to call getFP1 twice as it relies on getFT1, which is already memoized
         getFP1.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         //here however, no getFT4 was called before
         getFP4();   //this executes for 5+ms  (it invokes getFT1 for the first time)
         getFP4.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         //here, getFT4 was called implicitly before (via getFP4 above), so it is already memoized
         getFT4.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         getFF3();   //this executes for 5+ms
         getFF3.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         //.GetFldType("F2") has been memoized up-front (in initial dictionary), so that even 1st execution retrieves it from cache
         getFT2.ExecutionTime().Should().BeLessThan(4.Milliseconds());

         getFT1.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFT2.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFT4.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFF1.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFF3.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFP1.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFP4.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFP1.ExecutionTime().Should().BeLessThan(4.Milliseconds());
         getFT1.ExecutionTime().Should().BeLessThan(4.Milliseconds());
      }

      [TestMethod]
      public void GetFldCreator_CalledMultipleTimes_MemoizedAsExpected()
      {
         //arrange
         //  default settings, but methods take long time (9ms delay):
         Func<string, ItemType> fldTypeFunc = key => { Thread.Sleep(9); return ItemType.String; };
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>() { { "F2", ItemType.Int } });
         Func<string, string> fldFormatFunc = key => { Thread.Sleep(9); return string.Empty; };
         var initFldFormats = new ConcurrentDictionary<string, string>();

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);
         Action getFT1 = () => typeDefs.GetFldType("F1");
         Action getFF1 = () => typeDefs.GetFldFormat("F1");
         Action getFP1 = () => typeDefs.GetFldParser("F1")("anything");

         Action getFC1 = () => typeDefs.GetFldCreator("F1")("anyvalue");
         Action getFC11 = () => typeDefs.GetFldCreator("F1")("anotherval");

         //assert
         getFC1(); //this executes for 18+ms (implicit getFT1 + getFF1)
         getFP1.ExecutionTime().Should().BeLessThan(8.Milliseconds());
         getFT1.ExecutionTime().Should().BeLessThan(8.Milliseconds());
         getFT1.ExecutionTime().Should().BeLessThan(8.Milliseconds());
         getFF1.ExecutionTime().Should().BeLessThan(8.Milliseconds());
         getFC11.ExecutionTime().Should().BeLessThan(8.Milliseconds());
      }


      [TestMethod]
      public void GetFldCreator_SampleDefinions_CorrectData()
      {
         //arrange
         // somewhat typical settings (starting w/M_ = money, anything else Integer, plus some exceptions in initial values):
         // note that TypeDefinitions does not require types and formats to be setup for identical set of keys like below,
         //  (but this is how the data gets lined up by the EtlOrchestrator)
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("M_") ? ItemType.Decimal : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>()
                                                           {
                                                              { "M_Date", ItemType.DateTime },
                                                              { "IntFld", ItemType.Int },
                                                              { "BoolFld", ItemType.Bool }
                                                           });
         Func<string, string> fldFormatFunc = key => key.StartsWith("M_") ? "$##0.00" : string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>(new Dictionary<string, string>()
                                                           {
                                                              { "M_Date", "yymmdd" },
                                                              { "IntFld", "000,000" }
                                                           });

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //assert
         typeDefs.GetFldCreator("undef").Should().BeOfType(typeof(Func<string, IItem>));
         var itm = typeDefs.GetFldCreator("undef")("blah");
         itm.Should().BeOfType(typeof(KeyValItem<string>));
         itm.Key.Should().Be("undef");
         itm.Value.Should().Be("blah");
         itm.ItemDef.Type.Should().Be(ItemType.String);
         itm.ItemDef.Format.Should().Be(string.Empty);
         var sitm = itm as KeyValItem<string>;
         sitm.Value.Should().Be("blah");

         typeDefs.GetFldCreator("M_Date").Should().BeOfType(typeof(Func<string, IItem>));
         itm = typeDefs.GetFldCreator("M_Date")("9-MAY-1925");
         itm.Should().BeOfType(typeof(KeyValItem<DateTime>));
         itm.Key.Should().Be("M_Date");
         itm.Value.Should().Be(DateTime.Parse("5/9/1925"));

         itm.ItemDef.Type.Should().Be(ItemType.DateTime);
         itm.ItemDef.Format.Should().Be("yymmdd");
         var ditm = itm as KeyValItem<DateTime>;
         ditm.Value.Should().Be(DateTime.Parse("5/9/1925"));

         typeDefs.GetFldCreator("IntFld").Should().BeOfType(typeof(Func<string, IItem>));
         itm = typeDefs.GetFldCreator("IntFld")("144");
         itm.Should().BeOfType(typeof(KeyValItem<int>));
         itm.Key.Should().Be("IntFld");
         itm.Value.Should().Be(144);

         typeDefs.GetFldCreator("BoolFld").Should().BeOfType(typeof(Func<string, IItem>));
         itm = typeDefs.GetFldCreator("BoolFld")("True");
         itm.Should().BeOfType(typeof(KeyValItem<bool>));
         itm.Key.Should().Be("BoolFld");
         itm.Value.Should().Be(true);
      }


      [TestMethod]
      public void GetFldCreatorEx_SampleDefinions_CorrectData()
      {
         //arrange
         // somewhat typical settings (starting w/M_ = money, anything else Integer, plus some exceptions in initial values):
         // note that TypeDefinitions does not require types and formats to be setup for identical set of keys like below,
         //  (but this is how the data gets lined up by the EtlOrchestrator)
         Func<string, ItemType> fldTypeFunc = key => key.StartsWith("M_") ? ItemType.Decimal : ItemType.String;
         var initFldTypes = new ConcurrentDictionary<string, ItemType>(new Dictionary<string, ItemType>()
                                                           {
                                                              { "M_Date", ItemType.DateTime },
                                                              { "IntFld", ItemType.Int },
                                                              { "BoolFld", ItemType.Bool }
                                                           });
         Func<string, string> fldFormatFunc = key => key.StartsWith("M_") ? "$##0.00" : string.Empty;
         var initFldFormats = new ConcurrentDictionary<string, string>(new Dictionary<string, string>()
                                                           {
                                                              { "M_Date", "yymmdd" },
                                                              { "IntFld", "000,000" }
                                                           });

         //act
         var typeDefs = new TypeDefinitions(fldTypeFunc, initFldTypes, fldFormatFunc, initFldFormats);

         //assert
         typeDefs.GetFldCreatorEx("undef").Should().BeOfType(typeof(Func<object, IItem>));
         var itm = typeDefs.GetFldCreatorEx("undef")("blah");
         itm.Should().BeOfType(typeof(KeyValItem<string>));
         itm.Key.Should().Be("undef");
         itm.Value.Should().Be("blah");
         itm.ItemDef.Type.Should().Be(ItemType.String);
         itm.ItemDef.Format.Should().Be(string.Empty);
         var sitm = itm as KeyValItem<string>;
         sitm.Value.Should().Be("blah");

         typeDefs.GetFldCreatorEx("M_Date").Should().BeOfType(typeof(Func<object, IItem>));
         itm = typeDefs.GetFldCreatorEx("M_Date")(DateTime.Parse("5/9/1925"));
         itm.Should().BeOfType(typeof(KeyValItem<DateTime>));
         itm.Key.Should().Be("M_Date");
         itm.Value.Should().Be(DateTime.Parse("5/9/1925"));

         itm.ItemDef.Type.Should().Be(ItemType.DateTime);
         itm.ItemDef.Format.Should().Be("yymmdd");
         var ditm = itm as KeyValItem<DateTime>;
         ditm.Value.Should().Be(DateTime.Parse("5/9/1925"));

         typeDefs.GetFldCreatorEx("IntFld").Should().BeOfType(typeof(Func<object, IItem>));
         itm = typeDefs.GetFldCreatorEx("IntFld")(144);
         itm.Should().BeOfType(typeof(KeyValItem<int>));
         itm.Key.Should().Be("IntFld");
         itm.Value.Should().Be(144);

         typeDefs.GetFldCreatorEx("BoolFld").Should().BeOfType(typeof(Func<object, IItem>));
         itm = typeDefs.GetFldCreatorEx("BoolFld")(true);
         itm.Should().BeOfType(typeof(KeyValItem<bool>));
         itm.Key.Should().Be("BoolFld");
         itm.Value.Should().Be(true);
      }

   }
}