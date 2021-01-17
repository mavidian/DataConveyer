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


      }  //Initialize


      [DataTestMethod]
      [DataRow("Test_01")]
      [DataRow("Test_02")]
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
      public void JsonWritingAsync_EndToEnd_CorrectData(string testCase)
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
         inputRecs.ForEach(async r => await xrecordConsumer.SendNextLineAsync(Tuple.Create((ExternalLine)r, dummyTargetNo)));
         xrecordConsumer.ConcludeDispensingAsync().Wait();  //note that normally EOD marks are handled by LineDispenser
         var actual = output.ToString();

         //assert
         actual.Should().Be(expected);
      }

   }
}
