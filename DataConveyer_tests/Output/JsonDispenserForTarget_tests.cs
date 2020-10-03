//JsonDispenserForTarget_tests.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   public class JsonDispenserForTarget_tests
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
         //"Happy path" test
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
             "CollectionNode|Members,RecordNode|Member,IndentChars|   ",
             //expected output
             @"{
   ""Members"": {
      ""Member"": [
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
      ]
   }
}"
                                                 )  //Tuple.Create
                          );  //Add Test_01
#endregion Test_01


#region Test_02
         //No IndentChars setting, i.e. no "pretty print"
         _testDataRepo.Add("Test_02", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
                           )
             },
             //settings:
             "CollectionNode|Members,RecordNode|Member",
             //expected output
             @"{""Members"":{""Member"":[{""ID"":""1"",""FName"":""Paul"",""LName"":""Smith"",""DOB"":""1/12/1988""}]}}"
                                                 )  //Tuple.Create
                          );  //Add Test_02
#endregion Test_02


         // Test_3 for XML writing uses Unix line endings (NewLineChars|\n); there is no equivalent way for JSON as Newtonsoft JSON.NET always uses Environment.NewLine for pretty-print.
         // Conceivably, a derived JsonTextWriter class (JSON.NET) could be created with the WriteIndent method overridden.


#region Test_03
         // no collection
         _testDataRepo.Add("Test_03", Tuple.Create(
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
             "RecordNode|Member,IndentChars|\t",
             //expected output
             @"{
	""Member"": [
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
	]
}"
                                                 )  //Tuple.Create
                          );  //Add Test_03
#endregion Test_03


#region Test_04
         // respect clusters
         _testDataRepo.Add("Test_04", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member,IndentChars|  ",
             //expected output
             @"{
  ""Members"": {
    ""Family"": [
      {
        ""Member"": [
          {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988""
          }
        ]
      },
      {
        ""Member"": [
          {
            ""ID"": ""2"",
            ""FName"": ""John"",
            ""LName"": ""Green"",
            ""DOB"": ""8/23/1967""
          },
          {
            ""ID"": ""4"",
            ""FName"": ""Johnny"",
            ""LName"": ""Green"",
            ""DOB"": ""5/3/1997""
          }
        ]
      },
      {
        ""Member"": [
          {
            ""ID"": ""3"",
            ""FName"": ""Joseph"",
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        ]
      }
    ]
  }
}"
                                                 )  //Tuple.Create
                          );  //Add Test_04
#endregion Test_04


#region Test_05
         // respect clusters, no collection
         _testDataRepo.Add("Test_05", Tuple.Create(
             //records to output
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
             //settings:
             "ClusterNode|Family,RecordNode|Member,IndentChars|  ",
             //expected output
             @"{
  ""Family"": [
    {
      ""Member"": [
        {
          ""ID"": ""1"",
          ""FName"": ""Paul"",
          ""LName"": ""Smith"",
          ""DOB"": ""1/12/1988""
        }
      ]
    },
    {
      ""Member"": [
        {
          ""ID"": ""2"",
          ""FName"": ""John"",
          ""LName"": ""Green"",
          ""DOB"": ""8/23/1967""
        },
        {
          ""ID"": ""4"",
          ""FName"": ""Johnny"",
          ""LName"": ""Green"",
          ""DOB"": ""5/3/1997""
        }
      ]
    },
    {
      ""Member"": [
        {
          ""ID"": ""3"",
          ""FName"": ""Joseph"",
          ""LName"": ""Doe"",
          ""DOB"": ""11/6/1994""
        }
      ]
    }
  ]
}"
                                                 )  //Tuple.Create
                          );  //Add Test_05
         #endregion Test_05


#region Test_06
         // multi-level nodes with clusters
         _testDataRepo.Add("Test_06", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("Weight","180" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("Weight","195" as object),
                                                               Tuple.Create("EyeColor","Brown" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
             //settings:
            "CollectionNode|Root/Members,ClusterNode|Group/Subgroup/Family,RecordNode|Data/Member,IndentChars|  ",
             //expected output
             @"{
  ""Root"": {
    ""Members"": {
      ""Group"": {
        ""Subgroup"": {
          ""Family"": [
            {
              ""Data"": {
                ""Member"": [
                  {
                    ""ID"": ""1"",
                    ""FName"": ""Paul"",
                    ""LName"": ""Smith"",
                    ""Weight"": ""180"",
                    ""DOB"": ""1/12/1988""
                  }
                ]
              }
            },
            {
              ""Data"": {
                ""Member"": [
                  {
                    ""ID"": ""2"",
                    ""FName"": ""John"",
                    ""LName"": ""Green"",
                    ""DOB"": ""8/23/1967""
                  },
                  {
                    ""ID"": ""4"",
                    ""FName"": ""Johnny"",
                    ""LName"": ""Green"",
                    ""DOB"": ""5/3/1997""
                  }
                ]
              }
            },
            {
              ""Data"": {
                ""Member"": [
                  {
                    ""ID"": ""3"",
                    ""FName"": ""Joseph"",
                    ""LName"": ""Doe"",
                    ""Weight"": ""195"",
                    ""EyeColor"": ""Brown"",
                    ""DOB"": ""11/6/1994""
                  }
                ]
              }
            }
          ]
        }
      }
    }
  }
}"
                                                 )  //Tuple.Create
                          );  //Add Test_06
#endregion Test_06


#region Test_07
         // multi-level nodes, no clusters
         _testDataRepo.Add("Test_07", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("Weight","180" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object)
                                                             }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("FName","Johnny" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "5/3/1997" as object)
                                                             }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("FName","Joseph" as object),
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("Weight","195" as object),
                                                               Tuple.Create("EyeColor","Brown" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
, 3                            )
             },
            //settings:
            "CollectionNode|Root/Members/X,RecordNode|Data/Member,IndentChars|  ",
             //expected output
             @"{
  ""Root"": {
    ""Members"": {
      ""X"": {
        ""Data"": {
          ""Member"": [
            {
              ""ID"": ""1"",
              ""FName"": ""Paul"",
              ""LName"": ""Smith"",
              ""Weight"": ""180"",
              ""DOB"": ""1/12/1988""
            },
            {
              ""ID"": ""2"",
              ""FName"": ""John"",
              ""LName"": ""Green"",
              ""DOB"": ""8/23/1967""
            },
            {
              ""ID"": ""4"",
              ""FName"": ""Johnny"",
              ""LName"": ""Green"",
              ""DOB"": ""5/3/1997""
            },
            {
              ""ID"": ""3"",
              ""FName"": ""Joseph"",
              ""LName"": ""Doe"",
              ""Weight"": ""195"",
              ""EyeColor"": ""Brown"",
              ""DOB"": ""11/6/1994""
            }
          ]
        }
      }
    }
  }
}"
                                                 )  //Tuple.Create
                          );  //Add Test_07
#endregion Test_07


#region Test_08
         // no collection, empty RecordNode
         _testDataRepo.Add("Test_08", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight","180" as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create("Weight","195" as object),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "RecordNode|,IndentChars|  ",
             //expected output
             @"[
  {
    ""ID"": ""1"",
    ""FName"": ""Paul"",
    ""LName"": ""Smith"",
    ""Weight"": ""180"",
    ""DOB"": ""1/12/1988""
  },
  {
    ""ID"": ""2"",
    ""FName"": ""John"",
    ""LName"": ""Green"",
    ""DOB"": ""8/23/1967""
  },
  {
    ""ID"": ""4"",
    ""FName"": ""Johnny"",
    ""LName"": ""Green"",
    ""DOB"": ""5/3/1997""
  },
  {
    ""ID"": ""3"",
    ""FName"": ""Joseph"",
    ""LName"": ""Doe"",
    ""Weight"": ""195"",
    ""EyeColor"": ""Brown"",
    ""DOB"": ""11/6/1994""
  }
]"
                                                 )  //Tuple.Create
                          );  //Add Test_08
#endregion Test_08


#region Test_09
         // no collection, observe clusters, empty ClusterNode and RecordNode
         _testDataRepo.Add("Test_09", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight","180" as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create("Weight","195" as object),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "ClusterNode|,RecordNode|,IndentChars|  ",
             //expected output
             @"[
  [
    {
      ""ID"": ""1"",
      ""FName"": ""Paul"",
      ""LName"": ""Smith"",
      ""Weight"": ""180"",
      ""DOB"": ""1/12/1988""
    }
  ],
  [
    {
      ""ID"": ""2"",
      ""FName"": ""John"",
      ""LName"": ""Green"",
      ""DOB"": ""8/23/1967""
    },
    {
      ""ID"": ""4"",
      ""FName"": ""Johnny"",
      ""LName"": ""Green"",
      ""DOB"": ""5/3/1997""
    }
  ],
  [
    {
      ""ID"": ""3"",
      ""FName"": ""Joseph"",
      ""LName"": ""Doe"",
      ""Weight"": ""195"",
      ""EyeColor"": ""Brown"",
      ""DOB"": ""11/6/1994""
    }
  ]
]"
                                                 )  //Tuple.Create
                          );  //Add Test_09
#endregion Test_09


#region Test_10
         // no collection, observe clusters, empty RecordNode
         _testDataRepo.Add("Test_10", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight","180" as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create("Weight","195" as object),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "ClusterNode|Family,RecordNode|,IndentChars|  ",
             //expected output
             @"{
  ""Family"": [
    [
      {
        ""ID"": ""1"",
        ""FName"": ""Paul"",
        ""LName"": ""Smith"",
        ""Weight"": ""180"",
        ""DOB"": ""1/12/1988""
      }
    ],
    [
      {
        ""ID"": ""2"",
        ""FName"": ""John"",
        ""LName"": ""Green"",
        ""DOB"": ""8/23/1967""
      },
      {
        ""ID"": ""4"",
        ""FName"": ""Johnny"",
        ""LName"": ""Green"",
        ""DOB"": ""5/3/1997""
      }
    ],
    [
      {
        ""ID"": ""3"",
        ""FName"": ""Joseph"",
        ""LName"": ""Doe"",
        ""Weight"": ""195"",
        ""EyeColor"": ""Brown"",
        ""DOB"": ""11/6/1994""
      }
    ]
  ]
}"
                                                 )  //Tuple.Create
                          );  //Add Test_10
         #endregion Test_10         


#region Test_11
         // multi-level nodes, clusters, some nodes empty (i.e. arrays, not objects)
         _testDataRepo.Add("Test_11", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight","180" as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create("Weight","195" as object),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "CollectionNode|Root//Members/,ClusterNode|/Group/Subgroup///Family,RecordNode|Data/Member/,IndentChars|  ",
             //expected output
             @"{
  ""Root"": [
    {
      ""Members"": [
        [
          {
            ""Group"": {
              ""Subgroup"": [
                [
                  {
                    ""Family"": [
                      {
                        ""Data"": {
                          ""Member"": [
                            [
                              {
                                ""ID"": ""1"",
                                ""FName"": ""Paul"",
                                ""LName"": ""Smith"",
                                ""Weight"": ""180"",
                                ""DOB"": ""1/12/1988""
                              }
                            ]
                          ]
                        }
                      },
                      {
                        ""Data"": {
                          ""Member"": [
                            [
                              {
                                ""ID"": ""2"",
                                ""FName"": ""John"",
                                ""LName"": ""Green"",
                                ""DOB"": ""8/23/1967""
                              },
                              {
                                ""ID"": ""4"",
                                ""FName"": ""Johnny"",
                                ""LName"": ""Green"",
                                ""DOB"": ""5/3/1997""
                              }
                            ]
                          ]
                        }
                      },
                      {
                        ""Data"": {
                          ""Member"": [
                            [
                              {
                                ""ID"": ""3"",
                                ""FName"": ""Joseph"",
                                ""LName"": ""Doe"",
                                ""Weight"": ""195"",
                                ""EyeColor"": ""Brown"",
                                ""DOB"": ""11/6/1994""
                              }
                            ]
                          ]
                        }
                      }
                    ]
                  }
                ]
              ]
            }
          }
        ]
      ]
    }
  ]
}"
                                                 )  //Tuple.Create
                          );  //Add Test_11
#endregion Test_11


#region Test_12
         // no collection, cluster or record node
         //Special Case - output contains multiple root elements (technically not a valid JSON)
         // also note typed (non string) objects
         _testDataRepo.Add("Test_12", Tuple.Create(
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID",1 as object),
                                                                  Tuple.Create("FName","Paul" as object),
                                                                  Tuple.Create("LName","Smith" as object),
                                                                  Tuple.Create("Weight",180 as object),
                                                                  Tuple.Create("DOB", "1/12/1988" as object)
                                                                }
, 1                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID",2 as object),
                                                                  Tuple.Create("FName","John" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "8/23/1967" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID",4 as object),
                                                                  Tuple.Create("FName","Johnny" as object),
                                                                  Tuple.Create("LName","Green" as object),
                                                                  Tuple.Create("DOB", "5/3/1997" as object)
                                                                }
, 2                            ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID",3 as object),
                                                                  Tuple.Create("FName","Joseph" as object),
                                                                  Tuple.Create("LName","Doe" as object),
                                                                  Tuple.Create<string,object>("Weight",null),
                                                                  Tuple.Create("EyeColor","Brown" as object),
                                                                  Tuple.Create("DOB", "11/6/1994" as object)
                                                                }
, 3                            )
             },
             //settings:
             "IndentChars|  ",
             //expected output
             @"{
  ""ID"": 1,
  ""FName"": ""Paul"",
  ""LName"": ""Smith"",
  ""Weight"": 180,
  ""DOB"": ""1/12/1988""
}
{
  ""ID"": 2,
  ""FName"": ""John"",
  ""LName"": ""Green"",
  ""DOB"": ""8/23/1967""
}
{
  ""ID"": 4,
  ""FName"": ""Johnny"",
  ""LName"": ""Green"",
  ""DOB"": ""5/3/1997""
}
{
  ""ID"": 3,
  ""FName"": ""Joseph"",
  ""LName"": ""Doe"",
  ""Weight"": null,
  ""EyeColor"": ""Brown"",
  ""DOB"": ""11/6/1994""
}"
                                                 )  //Tuple.Create
                          );  //Add Test_12
         #endregion Test_12

      }  //Initialize


      [DataTestMethod]
      [DataRow("Test_01")]
      [DataRow("Test_02")]
      [DataRow("Test_03")]
      [DataRow("Test_04")]
      [DataRow("Test_05")]
      [DataRow("Test_06")]
      [DataRow("Test_07")]
      [DataRow("Test_08")]
      [DataRow("Test_09")]
      [DataRow("Test_10")]
      [DataRow("Test_11")]
      [DataRow("Test_12")]
      public void JsonWriting_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of JSON writing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new JsonDispenserForTarget(output, dummyTargetNo, settings, false);
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
      [DataRow("Test_07")]
      [DataRow("Test_08")]
      [DataRow("Test_09")]
      [DataRow("Test_10")]
      [DataRow("Test_11")]
      [DataRow("Test_12")]
      public void JsonWritingAsync_EndToEnd_CorrectData(string testCase)
      {
         //This is an end-to-end integration test of JSON writing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputRecs = testData.Item1;
         var settings = testData.Item2;
         var dummyTargetNo = 15;
         var output = new StringWriter();
         var xrecordConsumer = new JsonDispenserForTarget(output, dummyTargetNo, settings, true);
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
