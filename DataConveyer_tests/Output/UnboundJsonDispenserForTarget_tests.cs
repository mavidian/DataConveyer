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


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<List<Xrecord>, string, string>>();

#region Test_01
         //"Happy path", all defaults
         _testDataRepo.Add("Test_01", Tuple.Create(
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
                          );  //Add Test_01
#endregion Test_01


#region Test_02
         //"Happy path", ProduceMultipleObjects
         _testDataRepo.Add("Test_02", Tuple.Create(
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
                          );  //Add Test_02
#endregion Test_02


#region Test_03
         //"Happy path", ProduceClusters
         _testDataRepo.Add("Test_03", Tuple.Create(
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
                          );  //Add Test_03
#endregion Test_03


#region Test_04
         //"Happy path", ProduceClusters, ProduceMultipleObjects
         _testDataRepo.Add("Test_04", Tuple.Create(
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
                          );  //Add Test_04
#endregion Test_04


#region Test_05
         //Complex records (sorted)
         _testDataRepo.Add("Test_05", Tuple.Create(
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
                          );  //Add Test_05
         #endregion Test_05



         #region Test_06
         //Complex records, columns not presorted (not a recommended practice - elements are fragmented, but improves performance if columns are already groupped by nesting hierarchy).
         _testDataRepo.Add("Test_06", Tuple.Create(
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
                          );  //Add Test_06
         #endregion Test_06


      }  //Initialize


      [DataTestMethod]
      [DataRow("Test_01")]
      [DataRow("Test_02")]
      [DataRow("Test_03")]
      [DataRow("Test_04")]
      [DataRow("Test_05")]
      [DataRow("Test_06")]
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
      [DataRow("Test_01")]
      [DataRow("Test_02")]
      [DataRow("Test_03")]
      [DataRow("Test_04")]
      [DataRow("Test_05")]
      [DataRow("Test_06")]
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

   }
}
