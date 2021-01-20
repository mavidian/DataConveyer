//UnboundJsonDispenserForTarget_tests.cs
//
// Copyright © 2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataConveyer_tests.Output
{
   [TestClass]
   public class UnboundJsonDispenserForTarget_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. Members_1_
                         Tuple<List<Xrecord>,  // records to output
                               string,         // settings
                               string          // expected output
                              >
                        > _testDataRepo;

      //Test data for PresortData method (Tuple's Item2 is integer indicating order after sort)
      private Dictionary<string, List<Tuple<string, object>>> _sortTestData;

      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<List<Xrecord>, string, string>>();
         _sortTestData = new Dictionary<string, List<Tuple<string, object>>>();

#region TestCase_01
         //"Happy path", all defaults
         _testDataRepo.Add("TestCase_01", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""ID"": ""1"",
      ""FName"": ""Paul"",
      ""LName"": ""Smith"",
      ""DOB"": ""1/12/1988""
   },
   {
      ""ID"": ""2"",
      ""FName"": ""John"",
      ""LName"": ""Green"",
      ""DOB"": ""8/23/1967""
   },
   {
      ""ID"": ""3"",
      ""FName"": ""Joseph"",
      ""LName"": ""Doe"",
      ""DOB"": ""11/6/1994""
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_01
#endregion TestCase_01


#region TestCase_02
         //"Happy path", ProduceMultipleObjects
         _testDataRepo.Add("TestCase_02", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             },
             //settings:
             "ProduceMultipleObjects,IndentChars|   ",
             //expected output
             @"{
   ""ID"": ""1"",
   ""FName"": ""Paul"",
   ""LName"": ""Smith"",
   ""DOB"": ""1/12/1988""
}
{
   ""ID"": ""2"",
   ""FName"": ""John"",
   ""LName"": ""Green"",
   ""DOB"": ""8/23/1967""
}
{
   ""ID"": ""3"",
   ""FName"": ""Joseph"",
   ""LName"": ""Doe"",
   ""DOB"": ""11/6/1994""
}"
                                                 )  //Tuple.Create
                          );  //Add TestCase_02
#endregion TestCase_02


#region TestCase_03
         //"Happy path", ProduceClusters
         _testDataRepo.Add("TestCase_03", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             },1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             },1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             },2
                           )
             },
             //settings:
             "ProduceClusters,IndentChars|   ",
             //expected output
             @"[
   [
      {
         ""ID"": ""1"",
         ""FName"": ""Paul"",
         ""LName"": ""Smith"",
         ""DOB"": ""1/12/1988""
      },
      {
         ""ID"": ""2"",
         ""FName"": ""John"",
         ""LName"": ""Green"",
         ""DOB"": ""8/23/1967""
      }
   ],
   [
      {
         ""ID"": ""3"",
         ""FName"": ""Joseph"",
         ""LName"": ""Doe"",
         ""DOB"": ""11/6/1994""
      }
   ]
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_03
#endregion TestCase_03


#region TestCase_04
         //"Happy path", ProduceClusters, ProduceMultipleObjects
         _testDataRepo.Add("TestCase_04", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             },1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             },2
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             },2
                           )
             },
             //settings:
             "ProduceClusters,ProduceMultipleObjects,IndentChars|   ",
             //expected output
             @"[
   {
      ""ID"": ""1"",
      ""FName"": ""Paul"",
      ""LName"": ""Smith"",
      ""DOB"": ""1/12/1988""
   }
]
[
   {
      ""ID"": ""2"",
      ""FName"": ""John"",
      ""LName"": ""Green"",
      ""DOB"": ""8/23/1967""
   },
   {
      ""ID"": ""3"",
      ""FName"": ""Joseph"",
      ""LName"": ""Doe"",
      ""DOB"": ""11/6/1994""
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_04
#endregion TestCase_04


#region TestCase_05
         //Complex records (sorted)
         _testDataRepo.Add("TestCase_05", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("description", "This complex object contains nested arrays and objects" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[0]", 3 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[1]", "string value" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][0]", "Array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][1]", "with" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][2]", "strings" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][3]", "in" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][4]", "inner" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].StringInObject", "in inner array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NumberInObjectInInnerArray", 42 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[0]", 101 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[1]", 66 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[2]", 888 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[3]", 5 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[0]", "just" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[1]", 4 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[2]", "fun" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[4]", "another string value" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.NumberInNestedObject", 24 as object),  // note the location of this item is outside of nesting hierarchy
                                                               Tuple.Create("InnerArrayWithManyTypes[2][5]", "array" as object),  // note the location of this item is outside of nesting hierarchy
                                                               Tuple.Create("InnerArrayWithManyTypes[5]", 33 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""name"": ""Jimmy"",
      ""city"": ""New York"",
      ""age"": 17
   },
   {
      ""description"": ""This complex object contains nested arrays and objects"",
      ""InnerArrayWithManyTypes"": [
         3,
         ""string value"",
         [
            ""Array"",
            ""with"",
            ""strings"",
            ""in"",
            ""inner"",
            ""array""
         ],
         {
            ""StringInObject"": ""in inner array"",
            ""NumberInObjectInInnerArray"": 42,
            ""ArrayWithNumbersInObjectInInnerArray"": [
               101,
               66,
               888,
               5
            ],
            ""NestedObject"": {
               ""ArrayInNestedObject"": [
                  ""just"",
                  4,
                  ""fun""
               ],
               ""NumberInNestedObject"": 24
            }
         },
         ""another string value"",
         33
      ]
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_05
#endregion TestCase_05


#region TestCase_06
         //Complex records, columns not presorted (not a recommended practice - elements are fragmented, but improves performance if columns are already groupped by nesting hierarchy).
         _testDataRepo.Add("TestCase_06", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("description", "This complex object contains nested arrays and objects" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[0]", 3 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[1]", "string value" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][0]", "Array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][1]", "with" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][2]", "strings" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][3]", "in" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[2][4]", "inner" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].StringInObject", "in inner array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NumberInObjectInInnerArray", 42 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[0]", 101 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[1]", 66 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[2]", 888 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[3]", 5 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[0]", "just" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[1]", 4 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[2]", "fun" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[4]", "another string value" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.NumberInNestedObject", 24 as object),  // note the location of this item is outside of nesting hierarchy
                                                               Tuple.Create("InnerArrayWithManyTypes[2][5]", "array" as object),  // note the location of this item is outside of nesting hierarchy
                                                               Tuple.Create("InnerArrayWithManyTypes[5]", 33 as object)
                                                             }
                           )
             },
             //settings:
             "SkipColumnPresorting,IndentChars|   ",
             //expected output
             @"[
   {
      ""name"": ""Jimmy"",
      ""city"": ""New York"",
      ""age"": 17
   },
   {
      ""description"": ""This complex object contains nested arrays and objects"",
      ""InnerArrayWithManyTypes"": [
         3,
         ""string value"",
         [
            ""Array"",
            ""with"",
            ""strings"",
            ""in"",
            ""inner""
         ],
         {
            ""StringInObject"": ""in inner array"",
            ""NumberInObjectInInnerArray"": 42,
            ""ArrayWithNumbersInObjectInInnerArray"": [
               101,
               66,
               888,
               5
            ],
            ""NestedObject"": {
               ""ArrayInNestedObject"": [
                  ""just"",
                  4,
                  ""fun""
               ]
            }
         },
         ""another string value"",
         {
            ""NestedObject"": {
               ""NumberInNestedObject"": 24
            }
         },
         [
            ""array""
         ],
         33
      ]
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_06
#endregion TestCase_06


#region TestCase_07
         //Simple tabular records
         _testDataRepo.Add("TestCase_07", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("Year","2009" as object),
                                                               Tuple.Create("Population",0 as object),
                                                               Tuple.Create("Drivers",3782284 as object),
                                                               Tuple.Create("Vehicles", 4610850 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("Year","2010" as object),
                                                               Tuple.Create("Population",4785514 as object),
                                                               Tuple.Create("Drivers",3805751 as object),
                                                               Tuple.Create("Vehicles", 4653840 as object)
                                                             }
                            ),
               new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State", "Wyoming" as object),
                                                               Tuple.Create("Year", "2010" as object),
                                                               Tuple.Create("Population", 582328 as object),
                                                               Tuple.Create("Drivers", 0 as object),
                                                               Tuple.Create("Vehicles", 0 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""State"": ""Alabama"",
      ""Year"": ""2009"",
      ""Population"": 0,
      ""Drivers"": 3782284,
      ""Vehicles"": 4610850
   },
   {
      ""State"": ""Alabama"",
      ""Year"": ""2010"",
      ""Population"": 4785514,
      ""Drivers"": 3805751,
      ""Vehicles"": 4653840
   },
   {
      ""State"": ""Wyoming"",
      ""Year"": ""2010"",
      ""Population"": 582328,
      ""Drivers"": 0,
      ""Vehicles"": 0
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_07
#endregion TestCase_07


#region TestCase_08
         //Complex tabular records, grouped by by year
         _testDataRepo.Add("TestCase_08", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("Year","2009" as object),
                                                               Tuple.Create("Year.Population",0 as object),
                                                               Tuple.Create("Year.Drivers",3782284 as object),
                                                               Tuple.Create("Year.Vehicles", 4610850 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("Year","2010" as object),
                                                               Tuple.Create("Year.Population",4785514 as object),
                                                               Tuple.Create("Year.Drivers",3805751 as object),
                                                               Tuple.Create("Year.Vehicles", 4653840 as object)
                                                             }
                            ),
               new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State", "Wyoming" as object),
                                                               Tuple.Create("Year", "2010" as object),
                                                               Tuple.Create("Year.Population", 582328 as object),
                                                               Tuple.Create("Year.Drivers", 0 as object),
                                                               Tuple.Create("Year.Vehicles", 0 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""State"": ""Alabama"",
      ""Year"": ""2009"",
      ""Population"": 0,
      ""Drivers"": 3782284,
      ""Vehicles"": 4610850
   },
   {
      ""State"": ""Alabama"",
      ""Year"": ""2010"",
      ""Population"": 4785514,
      ""Drivers"": 3805751,
      ""Vehicles"": 4653840
   },
   {
      ""State"": ""Wyoming"",
      ""Year"": ""2010"",
      ""Population"": 582328,
      ""Drivers"": 0,
      ""Vehicles"": 0
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_08
         #endregion TestCase_08


         #region TestCase_09
         //Complex tabular records, grouped by by state & year
         _testDataRepo.Add("TestCase_09", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("State.Year","2009" as object),
                                                               Tuple.Create("State.Year.Population",0 as object),
                                                               Tuple.Create("State.Year.Drivers",3782284 as object),
                                                               Tuple.Create("State.Year.Vehicles", 4610850 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("State.Year","2010" as object),
                                                               Tuple.Create("State.Year.Population",4785514 as object),
                                                               Tuple.Create("State.Year.Drivers",3805751 as object),
                                                               Tuple.Create("State.Year.Vehicles", 4653840 as object)
                                                             }
                            ),
               new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State", "Wyoming" as object),
                                                               Tuple.Create("State.Year", "2010" as object),
                                                               Tuple.Create("State.Year.Population", 582328 as object),
                                                               Tuple.Create("State.Year.Drivers", 0 as object),
                                                               Tuple.Create("State.Year.Vehicles", 0 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""State"": ""Alabama"",
      ""Year"": ""2009"",
      ""Population"": 0,
      ""Drivers"": 3782284,
      ""Vehicles"": 4610850
   },
   {
      ""State"": ""Alabama"",
      ""Year"": ""2010"",
      ""Population"": 4785514,
      ""Drivers"": 3805751,
      ""Vehicles"": 4653840
   },
   {
      ""State"": ""Wyoming"",
      ""Year"": ""2010"",
      ""Population"": 582328,
      ""Drivers"": 0,
      ""Vehicles"": 0
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_09
#endregion TestCase_09


// ---------------- Data for PresortItems test below ---------------
// Tuples: Item1=Key, Item2=Value
// Items need to be ordered by Key segments in order of appearance.
// Item values indicate the order of the items.


#region SortTest_01
         //simple keys
         _sortTestData.Add("SortTest_01", new List<Tuple<string, object>>() {
                                                                Tuple.Create("first", 1 as object),
                                                                Tuple.Create("second", 2 as object),
                                                                Tuple.Create("third", 3 as object)
                                                              });
#endregion SortTest_01


#region SortTest_02
         //complex keys
         _sortTestData.Add("SortTest_02", new List<Tuple<string, object>>() {
                                                                Tuple.Create("State",1 as object),
                                                                Tuple.Create("State.Year",2 as object),
                                                                Tuple.Create("State.Year.Population",3 as object),
                                                                Tuple.Create("State.Year.Drivers",4 as object),
                                                                Tuple.Create("State.Year.Vehicles", 5 as object)
                                                             });
         #endregion SortTest_02


         #region SortTest_03
         //complex keys, mixed order
         _sortTestData.Add("SortTest_03", new List<Tuple<string, object>>() {
                                                                Tuple.Create("State.Year.Population",1 as object),
                                                                Tuple.Create("State.Year",2 as object),
                                                                Tuple.Create("State.Year.Drivers",3 as object),
                                                                Tuple.Create("State",5 as object),  // all State.Year segments go first
                                                                Tuple.Create("State.Year.Vehicles", 4 as object)
                                                             });
         #endregion SortTest_03


         #region SortTest_04
         //complex keys with inner arrays
         _sortTestData.Add("SortTest_04", new List<Tuple<string, object>>() {
                                                                Tuple.Create("Years[0][1].Drivers",1 as object),
                                                                Tuple.Create("State",9 as object),
                                                                Tuple.Create("Years[1][0]",5 as object),
                                                                Tuple.Create("Years[0][0]",4 as object),
                                                                Tuple.Create("Years[0][1].Population",2 as object),
                                                                Tuple.Create("Years[0][1].Vehicle",3 as object),
                                                                Tuple.Create("Years[1][1].Drivers",6 as object),
                                                                Tuple.Create("Years[1][1].Vehicles",7 as object),
                                                                Tuple.Create("Years[1][1].Population",8 as object)
                                                             });
         #endregion SortTest_04


         #region SortTest_05
         //complex keys with arrays
         _sortTestData.Add("SortTest_05", new List<Tuple<string, object>>() {
                                                                Tuple.Create("description", 1 as object),
                                                                Tuple.Create("InnerArray[0]", 2 as object),
                                                                Tuple.Create("InnerArray[4]", 3 as object),  // note array indices don't matter (order of appearance instead)
                                                                Tuple.Create("InnerArray[4][0]", 4 as object),
                                                                Tuple.Create("InnerArray[4][2]", 5 as object),
                                                                Tuple.Create("InnerArray[4][1]", 6 as object),
                                                                Tuple.Create("InnerArray[4][4]", 7 as object),
                                                                Tuple.Create("InnerArray[3].String", 9 as object),
                                                                Tuple.Create("InnerArray[3].Number", 10 as object),
                                                                Tuple.Create("InnerArray[3].InnerArray[0]", 11 as object),  // OK to have the same named segments
                                                                Tuple.Create("InnerArray[3].InnerArray[1]", 12 as object),
                                                                Tuple.Create("InnerArray[3].InnerArray[3]", 13 as object),
                                                                Tuple.Create("InnerArray[3].NestedObject.NestedArray[1]", 15 as object),
                                                                Tuple.Create("InnerArray[3].NestedObject.NestedArray[2]", 16 as object),
                                                                Tuple.Create("InnerArray[4][3]", 8 as object),
                                                                Tuple.Create("InnerArray[3].NestedObject.NestedArray[0]", 17 as object),
                                                                Tuple.Create("InnerArray[2]", 18 as object),  // again, order of appearance, not an index (InnerArray[2] shows here for the 1st time)
                                                                Tuple.Create("InnerArray[3].InnerArray[2]", 14 as object)
                                                             });
#endregion SortTest_05


      }  //Initialize


      [DataTestMethod]
      [DataRow("TestCase_01")]
      [DataRow("TestCase_02")]
      [DataRow("TestCase_03")]
      [DataRow("TestCase_04")]
      [DataRow("TestCase_05")]
      [DataRow("TestCase_06")]
      [DataRow("TestCase_07")]
      [DataRow("TestCase_08")]
      [DataRow("TestCase_09")]
      public void JsonWriting_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of JSON writing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new UnboundJsonDispenserForTarget(output, dummyTargetNo, settings);
         var expected = testData.Item3;

         //act
         inputRecs.ForEach(r => xrecordConsumer.SendNextLine(Tuple.Create((ExternalLine)r, dummyTargetNo)));
         xrecordConsumer.ConcludeDispensing();  //note that normally EOD marks are handled by LineDispenser
         var actual = output.ToString();

         //assert
         actual.Should().Be(expected);
      }


      [DataTestMethod]
      [DataRow("TestCase_01")]
      [DataRow("TestCase_02")]
      [DataRow("TestCase_03")]
      [DataRow("TestCase_04")]
      [DataRow("TestCase_05")]
      [DataRow("TestCase_06")]
      public void JsonWritingAsync_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of asynchronous JSON writing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new UnboundJsonDispenserForTarget(output, dummyTargetNo, settings);
         var expected = testData.Item3;

         //act
         inputRecs.ForEach(async r => await xrecordConsumer.SendNextLineAsync(Tuple.Create((ExternalLine)r, dummyTargetNo)));
         xrecordConsumer.ConcludeDispensingAsync().Wait();  //note that normally EOD marks are handled by LineDispenser
         var actual = output.ToString();

         //assert
         actual.Should().Be(expected);
      }


      [DataTestMethod]
      [DataRow("SortTest_01")]
      [DataRow("SortTest_02")]
      [DataRow("SortTest_03")]
      [DataRow("SortTest_04")]
      [DataRow("SortTest_05")]
      public void PresortItems_VariousKeyData_CorrectOrder(string sortTestCase)
      {
         // Tuples: Item1=Key, Item2=Value
         // Items need to be ordered by Key segments in order of appearance.
         // Item values indicate the order of the items.

         //arrange
         var itemsToSort = _sortTestData[sortTestCase];

         //act
         var resultingItems = UnboundJsonDispenserForTarget.PresortItems(itemsToSort).ToList();

         //assert
         resultingItems.Count.Should().Be(itemsToSort.Count);
         for (int i = 1; i < resultingItems.Count; i++)
         {
            ((int)resultingItems[i].Item2).Should().Be(((int)resultingItems[i - 1].Item2) + 1);
         }
      }


      ////[TestMethod]
      ////public void PresortItems_ComplexItems_CorrectOrder()
      ////{
      ////   // Tuples: Item1=Key, Item2=Value
      ////   // Items need to be ordered by Key segments in order of appearance.
      ////   // Item values indicate the order of the items.

      ////   //arrange
      ////   var itemsToSort = new List<Tuple<string, object>>() { Tuple.Create("State",1 as object),
      ////                                                         Tuple.Create("State.Year.Population",3 as object),
      ////                                                         Tuple.Create("State.Year",2 as object),
      ////                                                         Tuple.Create("State.Year.Drivers",4 as object),
      ////                                                         Tuple.Create("State.Year.Vehicles", 5 as object)
      ////                                                      };

      ////   //act
      ////   var resultingItems = UnboundJsonDispenserForTarget.PresortItems(itemsToSort).ToList();

      ////   //assert
      ////   resultingItems.Count.Should().Be(itemsToSort.Count);
      ////   for (int i = 1; i < resultingItems.Count; i++)
      ////   {
      ////      ((int)resultingItems[i].Item2).Should().Be(((int)resultingItems[i - 1].Item2) + 1);
      ////   }
      ////}


      ////[TestMethod]
      ////public void PresortItems_ComplexItemsWithArrays_CorrectOrder()
      ////{
      ////   // Tuples: Item1=Key, Item2=Value
      ////   // Items need to be ordered by Key segments in order of appearance.
      ////   // Item values indicate the order of the items.

      ////   //arrange
      ////   var itemsToSort = new List<Tuple<string, object>>() { Tuple.Create("description", 1 as object),
      ////                                                         Tuple.Create("InnerArray[0]", 2 as object),
      ////                                                         Tuple.Create("InnerArray[4]", 3 as object),  // note array indices don't matter (order of appearance instead)
      ////                                                         Tuple.Create("InnerArray[4][0]", 4 as object),
      ////                                                         Tuple.Create("InnerArray[4][2]", 5 as object),
      ////                                                         Tuple.Create("InnerArray[4][1]", 6 as object),
      ////                                                         Tuple.Create("InnerArray[4][4]", 7 as object),
      ////                                                         Tuple.Create("InnerArray[3].String", 9 as object),
      ////                                                         Tuple.Create("InnerArray[3].Number", 10 as object),
      ////                                                         Tuple.Create("InnerArray[3].InnerArray[0]", 11 as object),  // OK to have the same named segments
      ////                                                         Tuple.Create("InnerArray[3].InnerArray[1]", 12 as object),
      ////                                                         Tuple.Create("InnerArray[3].InnerArray[3]", 13 as object),
      ////                                                         Tuple.Create("InnerArray[3].NestedObject.NestedArray[1]", 15 as object),
      ////                                                         Tuple.Create("InnerArray[3].NestedObject.NestedArray[2]", 16 as object),
      ////                                                         Tuple.Create("InnerArray[4][3]", 8 as object),
      ////                                                         Tuple.Create("InnerArray[3].NestedObject.NestedArray[0]", 17 as object),
      ////                                                         Tuple.Create("InnerArray[2]", 18 as object),  // again, order of appearance, not an index (InnerArray[2] shows here for the 1st time)
      ////                                                         Tuple.Create("InnerArray[3].InnerArray[2]", 14 as object)
      ////                                                       };

      ////   //act
      ////   var resultingItems = UnboundJsonDispenserForTarget.PresortItems(itemsToSort).ToList();

      ////   //assert
      ////   resultingItems.Count.Should().Be(itemsToSort.Count);
      ////   for (int i = 1; i < resultingItems.Count; i++)
      ////   {
      ////      ((int)resultingItems[i].Item2).Should().Be(((int)resultingItems[i - 1].Item2) + 1);
      ////   }
      ////}

   }
}
