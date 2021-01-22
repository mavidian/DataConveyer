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
using Newtonsoft.Json;
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
         //"Happy path", ProduceStandaloneObjects
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
             "ProduceStandaloneObjects,IndentChars|   ",
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
         //"Happy path", ProduceClusters, ProduceStandaloneObjects
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
             "ProduceClusters,ProduceStandaloneObjects,IndentChars|   ",
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
         // Simple records (no nesting)
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
         //Simple array
         _testDataRepo.Add("TestCase_08", Tuple.Create(
             //records to output
             new List<Xrecord>
             {

                 new Xrecord(new List<Tuple<string, object>>() {
                                                               Tuple.Create("Arr[0]", 33 as object)
                                                             }
                            )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"[
   {
      ""Arr"": [
         33
      ]
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_08
#endregion TestCase_08


#region TestCase_09
         //Simple object
         _testDataRepo.Add("TestCase_09", Tuple.Create(
             //records to output
             new List<Xrecord>
             {

                 new Xrecord(new List<Tuple<string, object>>() {
                                                               Tuple.Create("Obj.Prop", 55 as object)
                                                             }
                            )
             },
             //settings:
             "SkipColumnPresorting,IndentChars|   ",
             //expected output
             @"[
   {
      ""Obj"": {
         ""Prop"": 55
      }
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_09
#endregion TestCase_09


#region TestCase_10
         //Complex records, not a realistic scenario, but illustration of how Data Conveyer writes unbound JSON.
         //This is an example of bad input data that Data Conveyer is able to make sense of and create a valid JSON.
         //Look at the Year value/object. Basically, the same problem as in ErrTestCase_01; however, as the value comes first, an ambiguous Year element is created.
         _testDataRepo.Add("TestCase_10", Tuple.Create(
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
      ""Year"": {
         ""Drivers"": 3782284,
         ""Vehicles"": 4610850
      }
   },
   {
      ""State"": ""Alabama"",
      ""Year"": ""2010"",
      ""Population"": 4785514,
      ""Year"": {
         ""Drivers"": 3805751,
         ""Vehicles"": 4653840
      }
   },
   {
      ""State"": ""Wyoming"",
      ""Year"": ""2010"",
      ""Population"": 582328,
      ""Year"": {
         ""Drivers"": 0,
         ""Vehicles"": 0
      }
   }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_10
#endregion TestCase_10


#region TestCase_11
         // Records by state with nested objects by year
         _testDataRepo.Add("TestCase_11", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("2009.Population",0 as object),
                                                               Tuple.Create("2009.Drivers",3782284 as object),
                                                               Tuple.Create("2009.Vehicles", 4610850 as object),
                                                               Tuple.Create("2010.Population",4785514 as object),
                                                               Tuple.Create("2010.Drivers",3805751 as object),
                                                               Tuple.Create("2010.Vehicles", 4653840 as object)
                                                             }
                           ),
               new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State", "Arizona" as object),
                                                               Tuple.Create("2009.Population",0 as object),
                                                               Tuple.Create("2009.Drivers",4403390 as object),
                                                               Tuple.Create("2009.Vehicles", 4357630 as object),
                                                               Tuple.Create("2010.Population",6407342 as object),
                                                               Tuple.Create("2010.Drivers",4443647 as object),
                                                               Tuple.Create("2010.Vehicles", 4320010 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|  ",
             //expected output
             @"[
  {
    ""State"": ""Alabama"",
    ""2009"": {
      ""Population"": 0,
      ""Drivers"": 3782284,
      ""Vehicles"": 4610850
    },
    ""2010"": {
      ""Population"": 4785514,
      ""Drivers"": 3805751,
      ""Vehicles"": 4653840
    }
  },
  {
    ""State"": ""Arizona"",
    ""2009"": {
      ""Population"": 0,
      ""Drivers"": 4403390,
      ""Vehicles"": 4357630
    },
    ""2010"": {
      ""Population"": 6407342,
      ""Drivers"": 4443647,
      ""Vehicles"": 4320010
    }
  }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_11
#endregion TestCase_11


#region TestCase_12
         // A single record with nested objects by year and state (note that numbers are expressed as strings here)
         _testDataRepo.Add("TestCase_12", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("2009.Alabama.Population", "" as object),
                                                               Tuple.Create("2009.Alabama.Drivers","3782284" as object),
                                                               Tuple.Create("2009.Alabama.Vehicles", "4610850" as object),
                                                               Tuple.Create("2009.Arizona.Population","" as object),
                                                               Tuple.Create("2009.Arizona.Drivers","4403390" as object),
                                                               Tuple.Create("2009.Arizona.Vehicles","4357630" as object),
                                                               Tuple.Create("2010.Alabama.Population","4785514" as object),
                                                               Tuple.Create("2010.Alabama.Drivers","3805751" as object),
                                                               Tuple.Create("2010.Alabama.Vehicles", "4653840" as object),
                                                               Tuple.Create("2010.Arizona.Population","6407342" as object),
                                                               Tuple.Create("2010.Arizona.Drivers","4443647" as object),
                                                               Tuple.Create("2010.Arizona.Vehicles", "4320010" as object)
                                                            }
                           )
             },
             //settings:
             "ProduceStandaloneObjects,IndentChars|  ",
             //expected output
             @"{
  ""2009"": {
    ""Alabama"": {
      ""Population"": """",
      ""Drivers"": ""3782284"",
      ""Vehicles"": ""4610850""
    },
    ""Arizona"": {
      ""Population"": """",
      ""Drivers"": ""4403390"",
      ""Vehicles"": ""4357630""
    }
  },
  ""2010"": {
    ""Alabama"": {
      ""Population"": ""4785514"",
      ""Drivers"": ""3805751"",
      ""Vehicles"": ""4653840""
    },
    ""Arizona"": {
      ""Population"": ""6407342"",
      ""Drivers"": ""4443647"",
      ""Vehicles"": ""4320010""
    }
  }
}"
                                                 )  //Tuple.Create
                          );  //Add TestCase_12
         #endregion TestCase_12


#region TestCase_13
         // A single record with nested arrays of states and then years
         _testDataRepo.Add("TestCase_13", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("States[0].Alabama.Years[0].2009.Population", "" as object),
                                                               Tuple.Create("States[0].Alabama.Years[0].2009.Drivers","3782284" as object),
                                                               Tuple.Create("States[0].Alabama.Years[0].2009.Vehicles", "4610850" as object),
                                                               Tuple.Create("States[0].Alabama.Years[1].2010.Population","4785514" as object),
                                                               Tuple.Create("States[0].Alabama.Years[1].2010.Drivers","3805751" as object),
                                                               Tuple.Create("States[0].Alabama.Years[1].2010.Vehicles","4653840" as object),
                                                               Tuple.Create("States[1].Arizona.Years[0].2009.Population","" as object),
                                                               Tuple.Create("States[1].Arizona.Years[0].2009.Drivers","4403390" as object),
                                                               Tuple.Create("States[1].Arizona.Years[0].2009.Vehicles", "4357630" as object),
                                                               Tuple.Create("States[1].Arizona.Years[1].2010.Population","6407342" as object),
                                                               Tuple.Create("States[1].Arizona.Years[1].2010.Drivers","4443647" as object),
                                                               Tuple.Create("States[1].Arizona.Years[1].2010.Vehicles", "4320010" as object)
                                                            }
                           )
             },
             //settings:
             "ProduceStandaloneObjects,IndentChars|  ",
             //expected output
             @"{
  ""States"": [
    {
      ""Alabama"": {
        ""Years"": [
          {
            ""2009"": {
              ""Population"": """",
              ""Drivers"": ""3782284"",
              ""Vehicles"": ""4610850""
            }
          },
          {
            ""2010"": {
              ""Population"": ""4785514"",
              ""Drivers"": ""3805751"",
              ""Vehicles"": ""4653840""
            }
          }
        ]
      }
    },
    {
      ""Arizona"": {
        ""Years"": [
          {
            ""2009"": {
              ""Population"": """",
              ""Drivers"": ""4403390"",
              ""Vehicles"": ""4357630""
            }
          },
          {
            ""2010"": {
              ""Population"": ""6407342"",
              ""Drivers"": ""4443647"",
              ""Vehicles"": ""4320010""
            }
          }
        ]
      }
    }
  ]
}"
                                                 )  //Tuple.Create
                          );  //Add TestCase_13
         #endregion TestCase_13


#region TestCase_14
         // Records by year with nested arrays by state
         _testDataRepo.Add("TestCase_14", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("Year","2009" as object),
                                                               Tuple.Create("States[0][0]", "Alabama" as object),
                                                               Tuple.Create("States[0][1].Population",0 as object),
                                                               Tuple.Create("States[0][1].Drivers",3782284 as object),
                                                               Tuple.Create("States[0][1].Vehicles", 4610850 as object),
                                                               Tuple.Create("States[1][0]", "Arizona" as object),
                                                               Tuple.Create("States[1][1].Population", 0 as object),
                                                               Tuple.Create("States[1][1].Drivers",4403390 as object),
                                                               Tuple.Create("States[1][1].Vehicles", 4357630 as object)
                                                             }
                           ),
               new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("Year", "2010" as object),
                                                               Tuple.Create("States[0][0]", "Alabama" as object),
                                                               Tuple.Create("States[0][1].Population",4785514 as object),
                                                               Tuple.Create("States[0][1].Drivers",3805751 as object),
                                                               Tuple.Create("States[0][1].Vehicles", 4653840 as object),
                                                               Tuple.Create("States[1][0]", "Arizona" as object),
                                                               Tuple.Create("States[1][1].Population",6407342 as object),
                                                               Tuple.Create("States[1][1].Drivers",4443647 as object),
                                                               Tuple.Create("States[1][1].Vehicles", 4320010 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|  ",
             //expected output
             @"[
  {
    ""Year"": ""2009"",
    ""States"": [
      [
        ""Alabama"",
        {
          ""Population"": 0,
          ""Drivers"": 3782284,
          ""Vehicles"": 4610850
        }
      ],
      [
        ""Arizona"",
        {
          ""Population"": 0,
          ""Drivers"": 4403390,
          ""Vehicles"": 4357630
        }
      ]
    ]
  },
  {
    ""Year"": ""2010"",
    ""States"": [
      [
        ""Alabama"",
        {
          ""Population"": 4785514,
          ""Drivers"": 3805751,
          ""Vehicles"": 4653840
        }
      ],
      [
        ""Arizona"",
        {
          ""Population"": 6407342,
          ""Drivers"": 4443647,
          ""Vehicles"": 4320010
        }
      ]
    ]
  }
]"
                                                 )  //Tuple.Create
                          );  //Add TestCase_14
#endregion TestCase_14


// ---------------- Data for "error" tests below ---------------
// These tests do not yield JSON data; they are unusual sceanarios not typically encountered. Potentally, valid JSON, but not suppeorted by Data Conveyer.
// The tests are expected to throw JsonWriterException.


#region ErrTestCase_01
         //Conflict in Year element definition
         _testDataRepo.Add("ErrTestCase_01", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State","Alabama" as object),
                                                               Tuple.Create("Year.Population",0 as object),
                                                               Tuple.Create("Year","2009" as object),
                                                               Tuple.Create("Year.Drivers",3782284 as object),
                                                               Tuple.Create("Year.Vehicles", 4610850 as object)
                                                             }
                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"--- ambiguous definition of Year element (value and object); scenario not supported by Data Conveyer ---"
                                                 )  //Tuple.Create
                          );  //Add ErrTestCase_01
#endregion ErrTestCase_01


#region ErrTestCase_02
         //Conflict in State element definition
         _testDataRepo.Add("ErrTestCase_02", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("State.Year","2009" as object),
                                                               Tuple.Create("State.Year.Population",0 as object),
                                                               Tuple.Create("State.Year.Drivers",3782284 as object),
                                                               Tuple.Create("State.Year.Vehicles", 4610850 as object),
                                                               Tuple.Create("State","Alabama" as object)
                                                             }

                           )
             },
             //settings:
             "IndentChars|   ",
             //expected output
             @"--- ambiguous definition of State element (value and object); scenario not supported by Data Conveyer ---"
                                                 )  //Tuple.Create
                          );  //Add ErrTestCase_02
#endregion ErrTestCase_02


#region ErrTestCase_03
         //duplicate value
         _testDataRepo.Add("ErrTestCase_03", Tuple.Create(
             //records to output
             new List<Xrecord>
             {

                 new Xrecord(new List<Tuple<string, object>>() {
                                                               Tuple.Create("Val", 777 as object),
                                                               Tuple.Create("Val", 777 as object)
                                                             }
                            )
             },
             //settings:
             "SkipColumnPresorting,IndentChars|   ",  // note that an attempt to sort would result in stack overflow
             //expected output
             @"--- redundant definition of Val value; scenario not supported by Data Conveyer ---"

                                                 )  //Tuple.Create
                          );  //Add ErrTestCase_03
#endregion ErrTestCase_03



// ---------------- Data for PresortItems tests below ---------------
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
      [DataRow("TestCase_10")]
      [DataRow("TestCase_11")]
      [DataRow("TestCase_12")]
      [DataRow("TestCase_13")]
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
      [DataRow("ErrTestCase_01")]
      [DataRow("ErrTestCase_02")]
      [DataRow("ErrTestCase_03")]
      [ExpectedException(typeof(JsonWriterException))]
      public void JsonWriting_BadInput_ExceptionThrown(string testCase)
      {
         //This test attempts JSON writing where Xrecord data is not representable as JSON

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

         //assert - expect exception
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

   }
}
