//UnboundJsonFeederForSource_tests.cs
//
// Copyright © 2020-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Intake;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class UnboundJsonFeederForSource_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. SingleRecord
                         Tuple<string,         // input JSON
                               string,         // settings
                               List<Xrecord>   // expected
                              >
                        > _testDataRepo;


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<string, string, List<Xrecord>>>();

#region SingleRecord:
         //single record
         _testDataRepo.Add("SingleRecord", Tuple.Create(
//input:
@"{
      name: ""Jimmy"",
      city: ""New York"",
      age: 17
}",
             //settings:
             string.Empty,
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add SingleRecord
         #endregion SingleRecord


#region SetOfRecords:
         //set of top level JSON objects (technically not a valid JSON, but commonly used - interpreted the same as array of objects
         _testDataRepo.Add("SetOfRecords", Tuple.Create(
//input:
@"{
  name: ""Jimmy"",
  city: ""New York"",
  age: 17
}
{
  name: ""John"",
  city: ""New York"",
  age: 30
}
{
  name: ""Sue"",
  city: ""Chicago"",
  age: 20
}",
             //settings:
             string.Empty,
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }, 0
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","John" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 30 as object)
                                                             }, 0
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Sue" as object),
                                                               Tuple.Create("city","Chicago" as object),
                                                               Tuple.Create("age", 20 as object)
                                                             }, 0
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add SetOfRecords
#endregion SetOfRecords


#region SetOfClusters:
         //set of top level JSON arrays (technically not a valid JSON, but commonly used - interpreted the same as array of arrays
         _testDataRepo.Add("SetOfClusters", Tuple.Create(
//input:
@"[
  {
    name: ""Jimmy"",
    city: ""New York"",
    age: 17
  }
]
[
  {
    name: ""John"",
    city: ""New York"",
    age: 30
  },
  {
    name: ""Sue"",
    city: ""Chicago"",
    age: 20
  }
]",
             //settings:
             "DetectClusters",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }, 1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","John" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 30 as object)
                                                             }, 2
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Sue" as object),
                                                               Tuple.Create("city","Chicago" as object),
                                                               Tuple.Create("age", 20 as object)
                                                             }, 2
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add SetOfClusters
#endregion SetOfClusters


#region MixedClusters:
         //top level JSON object and array (technically not a valid JSON, but commonly used), object treated as a single record cluster
         _testDataRepo.Add("MixedClusters", Tuple.Create(
//input:
@"{
  name: ""Jimmy"",
  city: ""New York"",
  age: 17
}
[
  {
    name: ""John"",
    city: ""New York"",
    age: 30
  },
  {
    name: ""Sue"",
    city: ""Chicago"",
    age: 20
  }
]",
             //settings:
             "DetectClusters",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }, 1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","John" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 30 as object)
                                                             }, 2
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Sue" as object),
                                                               Tuple.Create("city","Chicago" as object),
                                                               Tuple.Create("age", 20 as object)
                                                             }, 2
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add MixedClusters
#endregion MixedClusters


#region ArrayOfClusters:
         //array of JSON arrays, each interpreted as a cluster
         _testDataRepo.Add("ArrayOfClusters", Tuple.Create(
//input:
@"[
  [
    {
      ""name"": ""Jimmy"",
      ""city"": ""New York"",
      ""age"": 17
    },
    {
      ""name"": ""John"",
      ""city"": ""New York"",
      ""age"": 30
    }
  ],
  [
    {
      ""name"": ""Sue"",
      ""city"": ""Chicago"",
      ""age"": 20
    }
  ]
]",
             //settings:
             "DetectClusters",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }, 1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","John" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 30 as object)
                                                             }, 1
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Sue" as object),
                                                               Tuple.Create("city","Chicago" as object),
                                                               Tuple.Create("age", 20 as object)
                                                             }, 2
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add ArrayOfClusters
#endregion ArrayOfClusters


#region ArrayWithNoRecords:
         //array with simple values (no records will be extracted)
         _testDataRepo.Add("ArrayWithNoRecords", Tuple.Create(
//input:
@"[
  ""Jimmy"",
  ""Peter"",
  [
    ""John"",
    ""Anna""
  ]
]",
             //settings:
             string.Empty,
             //expected:
             new List<Xrecord>() //expected (no records)
                                                             ) //Tuple.Create
                          );  //Add SimpleArray
#endregion SimpleArray


#region ArrayOfSimpleRecords:
         //array of simple records
         _testDataRepo.Add("ArrayOfSimpleRecords", Tuple.Create(
//input:
@"[
    {
      name: ""Jimmy"",
      city: ""New York"",
      age: 17
    },
    {
      name: ""John"",
      city: ""New York"",
      age: 30
    },
    {
      name: ""Sue"",
      city: ""Chicago"",
      age: 20
    }
]",
             //settings:
             string.Empty,
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Jimmy" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 17 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","John" as object),
                                                               Tuple.Create("city","New York" as object),
                                                               Tuple.Create("age", 30 as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("name","Sue" as object),
                                                               Tuple.Create("city","Chicago" as object),
                                                               Tuple.Create("age", 20 as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add ArrayOfSimpleRecords
#endregion ArrayOfSimpleRecords


#region ArrayOfComplexRecords:
         //array of complex records
         _testDataRepo.Add("ArrayOfComplexRecords", Tuple.Create(
//input:
@"[
  {
    ""name"": ""Jimmy"",
    ""city"": ""New York"",
    ""age"": 17
  },
  {
    ""description"": ""This complex object contains nested arrays and objects"",
    ""InnerArrayWithManyTypes"":
    [
       3,
       ""string value"",
       [ ""Array"", ""with"", ""strings"", ""in"", ""inner"", ""array"" ],
       {
             ""StringInObject"": ""in inner array"",
             ""NumberInObjectInInnerArray"": 42,
             ""ArrayWithNumbersInObjectInInnerArray"": [ 101, 66, 888, 5 ],
             ""NestedObject"": { ""NumberInNestedObject"": 24, ""ArrayInNestedObject"": [ ""just"", 4, ""fun"" ] }
       },
       ""another string value"",
       33
    ]
  }
]",
             //settings:
             string.Empty,
             //expected:
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
                                                               Tuple.Create("InnerArrayWithManyTypes[2][5]", "array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].StringInObject", "in inner array" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NumberInObjectInInnerArray", 42 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[0]", 101 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[1]", 66 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[2]", 888 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].ArrayWithNumbersInObjectInInnerArray[3]", 5 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.NumberInNestedObject", 24 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[0]", "just" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[1]", 4 as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[3].NestedObject.ArrayInNestedObject[2]", "fun" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[4]", "another string value" as object),
                                                               Tuple.Create("InnerArrayWithManyTypes[5]", 33 as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add ArrayOfComplexRecords
#endregion ArrayOfComplexRecords

      }


      [DataTestMethod]
      [DataRow("SingleRecord")]
      [DataRow("SetOfRecords")]
      [DataRow("SetOfClusters")]
      [DataRow("MixedClusters")]
      [DataRow("ArrayOfClusters")]
      [DataRow("ArrayWithNoRecords")]
      [DataRow("ArrayOfSimpleRecords")]
      [DataRow("ArrayOfComplexRecords")]
      public void UnboundJsonParsing_EndToEnd_CorrectData(string testCase)
      {
         //This is a series of end-to-end integration tests of unbound JSON parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputJSON = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 42;
         var xrecordSupplier = new UnboundJsonFeederForSource(new StringReader(inputJSON), dummySourceNo, settings);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item3;

         //act
         Xrecord elem;
         while ((elem = (Xrecord)xrecordSupplierPO.Invoke("SupplyNextXrecord")) != null)
         {
            actual.Add(elem);
         }

         //assert
         actual.Count.Should().Be(expected.Count);
         for (var i = 0; i < actual.Count; i++)
         {
            actual[i].Should().BeEquivalentTo(expected[i]);
         }
      }

      [DataTestMethod]
      [DataRow("SingleRecord")]
      [DataRow("SetOfRecords")]
      [DataRow("ArrayWithNoRecords")]
      [DataRow("ArrayOfSimpleRecords")]
      [DataRow("ArrayOfComplexRecords")]
      public void UnboundJsonParsingAsync_EndToEnd_CorrectData(string testCase)
      {
         //This is a series of end-to-end integration tests of asynchronous unbound JSON parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputJSON = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 42;
         var xrecordSupplier = new UnboundJsonFeederForSource(new StringReader(inputJSON), dummySourceNo, settings);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item3;

         //act
         Xrecord elem;
         while ((elem = ((Task<Xrecord>)xrecordSupplierPO.Invoke("SupplyNextXrecordAsync")).Result) != null)
         {
            actual.Add(elem);
         }

         //assert
         actual.Count.Should().Be(expected.Count);
         for (var i = 0; i < actual.Count; i++)
         {
            actual[i].Should().BeEquivalentTo(expected[i]);
         }
      }

   }
}
