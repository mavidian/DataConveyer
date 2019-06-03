//JsonFeederForSource_tests.cs
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
using Mavidian.DataConveyer.Intake;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class JsonFeederForSource_tests
   {
      // Test data is kept in this dictionary:
      private Dictionary<string,               // testCase, i.e. key (e.g. Members_1_
                         Tuple<string,         // inputXML
                               string,         // settings
                               List<Xrecord>   // expected
                              >
                        > _testDataRepo;


      [TestInitialize]
      public void Initialize()
      {
         _testDataRepo = new Dictionary<string, Tuple<string, string, List<Xrecord>>>();

#region Members_1.json:
         //simple collection node
         _testDataRepo.Add("Members_1", Tuple.Create(
//inputJSON:
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
}",
             //settings:
             "CollectionNode|Members,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1
#endregion Members_1.json


#region Members_1a.json
         //nested collection node
         _testDataRepo.Add("Members_1a", Tuple.Create(
//inputJSON:
@"{
	""Root"": {
		""Members"": [
			{
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
			},
			{
				""Member"": ""Same level, but ignored as not an object.""
			},
			{
				""Member"": { ""X"" : ""Same level, not ignored in JSON."" }
			}
		]
	}
}",
             //settings:
             "CollectionNode|Root/Members,RecordNode|Member",
             //expected:
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
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("X","Same level, not ignored in JSON." as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1a
#endregion Members_1a.json


#region Members_1a1.json
         //nested collection node w/consecutive arrays
         _testDataRepo.Add("Members_1a1", Tuple.Create(
//inputJSON:
@"{
   ""Root"": {
      ""Members"": [ [ [
         {
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
         },
         {
            ""Member"": { ""Desc"": ""This contents is not ignored in JSON."" }
         }
      ], [ { ""Member"": ""JSON includes this one too!""} ] ] ]
   }
}",
             //settings:
             "CollectionNode|Root/Members//,RecordNode|Member",
             //expected:
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
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("Desc","This contents is not ignored in JSON." as object),
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1a1
#endregion Members_1a1.json


#region Members_1c.json:
         //nested collection with distractors
         _testDataRepo.Add("Members_1c", Tuple.Create(
//inputJSON:
@"{
	""Root"": {
		""Distractor0"": """",
		""Distractor1"": """",
		""Distractor2"": {
			""_a"": ""B"",
			""_c"": ""D"",
			""__text"": ""blah""
		},
		""Members"": [
			{
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
				],
				""Distractor3"": ""blah3""
			},
			{
				""Distractor5"": ""blah5"",
				""Member"": ""This conetnts will be ignored.""
			}
		],
		""Distractor4"": ""blah4"",
		""Distractor6"": ""blah6""
	}
}",
             //settings:
             "CollectionNode|Root/Members,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1c
#endregion Members_1c.json


#region Members_1d.json:
         //redundant record nodes
         _testDataRepo.Add("Members_1d", Tuple.Create(
//inputJSON:
@"{
  ""Root"": {
    ""Members"": [
      {
        ""Member"": [
          {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988"",
            ""Empty1"": """",
            ""Empty2"": """"
          },
          {
            ""ID"": ""2"",
            ""FName"": ""John"",
            ""LName"": ""Green"",
            ""DOB"": ""8/23/1967""
          },
          {
            ""ID"": ""3"",
            ""FName"": [
              ""Joseph"",
              ""Dup'd Joseph""
            ],
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        ]
      },
      {
        ""Member"": {
          ""ID"": ""-1"",
          ""Data"": ""Same level, not ignored in JSON.""
        }
      },
      {
        ""Member"": {
          ""ID"": ""-2"",
          ""Data"": ""Same level, not ignored in JSON.""
         }
       }
    ]
  }
}",
             //settings:
             "CollectionNode|Root/Members,RecordNode|Member",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty1", "" as object),
                                                               Tuple.Create("Empty2", "" as object)
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
                                                               Tuple.Create("FName","Dup'd Joseph" as object),  //note that ActionOnDuplicateKey does not take effect until KeyValRecord ctor
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","-1" as object),
                                                               Tuple.Create("Data","Same level, not ignored in JSON." as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","-2" as object),
                                                               Tuple.Create("Data","Same level, not ignored in JSON." as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1d
#endregion Members_1d.json


#region Members_1e.json:
         //implied collection node
         _testDataRepo.Add("Members_1e", Tuple.Create(
//inputJSON:
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
}",
             //settings:
             "RecordNode|Member",  //"CollectionNode|Member,RecordNode|" would work here as well (but not "CollectionNode|,RecordNode|Member")
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1e
#endregion Members_1e.json


#region Members_1f.json:
         //implied collection and record nodes
         _testDataRepo.Add("Members_1f", Tuple.Create(
//inputJSON:
@"[
    {
      ""ID"": ""1"",
      ""FName"": ""Paul"",
      ""LName"": ""Smith"",
      ""DOB"": ""1/12/1988"",
      ""Empty1"": """",
      ""Empty2"": """"
    },
    {
      ""ID"": ""2"",
      ""FName"": ""John"",
      ""LName"": ""Green"",
      ""DOB"": ""8/23/1967""
    },
    {
      ""ID"": ""3"",
      ""FName"": [
        ""Joseph"",
        ""Dup'd Joseph""
      ],
      ""LName"": ""Doe"",
      ""DOB"": ""11/6/1994""
    }
]",
             //settings:
             "CollectionNode|,RecordNode|",  // here, collection node must be present
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty1", "" as object),
                                                               Tuple.Create("Empty2", "" as object)
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
                                                               Tuple.Create("FName","Dup'd Joseph" as object),  //note that ActionOnDuplicateKey does not take effect until KeyValRecord ctor
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1f
#endregion Members_1f.json


#region Members_1g.json:
         //multi-level RecordNode
         _testDataRepo.Add("Members_1g", Tuple.Create(
@"{
  ""Members"": {
    ""Member"": [
      {
        ""X"": {
          ""ID"": ""1"",
          ""FName"": ""Paul"",
          ""LName"": ""Smith"",
          ""DOB"": ""1/12/1988""
        }
      },
      {
        ""X"": {
          ""ID"": ""2"",
          ""FName"": ""John"",
          ""LName"": ""Green"",
          ""DOB"": ""8/23/1967""
        }
      },
      {
        ""X"": {
          ""ID"": ""3"",
          ""FName"": ""Joseph"",
          ""LName"": ""Doe"",
          ""DOB"": ""11/6/1994""
        }
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,RecordNode|Member/X",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_1g
#endregion Members_1g.json


#region Members_2.json:
         //This test respects clusters and assigns them to resulting records (ClusterNode setting is present)
         // (note that each cluster contains an array of records - even if there is a single record in cluster)
         _testDataRepo.Add("Members_2", Tuple.Create(
//inputJSON:
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
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2
#endregion Members_2.json


#region Members_2b.json:
         //This test respects clusters and assigns them to resulting records
         // unlike Members_2, if cluster contains a single record, the record is directly embedded in cluster (no single element array)
         _testDataRepo.Add("Members_2b", Tuple.Create(
//inputJSON:
@"{
	""Members"": {
		""Family"": [
			{
				""Member"": {
					""ID"": ""1"",
					""FName"": ""Paul"",
					""LName"": ""Smith"",
					""DOB"": ""1/12/1988""
				}
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
				""Member"": {
					""ID"": ""3"",
					""FName"": ""Joseph"",
					""LName"": ""Doe"",
					""DOB"": ""11/6/1994""
				}
			}
		]
	}
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2b
#endregion Members_2b.json


#region Members_2c.json:
         //This test respects clusters and assigns them to resulting records
         // in this tests clusters contain no arrays of records at all; instead, records are repeated objects (which result in non-unique key; BTW, this is allowed in JSON albeit not recommended)
         _testDataRepo.Add("Members_2c", Tuple.Create(
//inputJSON:
@"{
	""Members"": {
		""Family"": [
			{
				""Member"": {
					""ID"": ""1"",
					""FName"": ""Paul"",
					""LName"": ""Smith"",
					""DOB"": ""1/12/1988""
				}
			},
			{
				""Member"": {
						""ID"": ""2"",
						""FName"": ""John"",
						""LName"": ""Green"",
						""DOB"": ""8/23/1967""
					},
				""Member"": {
						""ID"": ""4"",
						""FName"": ""Johnny"",
						""LName"": ""Green"",
						""DOB"": ""5/3/1997""
					}
			},
			{
				""Member"": {
					""ID"": ""3"",
					""FName"": ""Joseph"",
					""LName"": ""Doe"",
					""DOB"": ""11/6/1994""
				}
			}
		]
	}
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2c
#endregion Members_2c.json


#region Members_2d.json:
         //This test respects clusters (ClusterNode setting is present, but CollectionNode is absent)
         // also last record is outside of cluster node (and hence ignored)
         _testDataRepo.Add("Members_2d", Tuple.Create(
//inputJSON:
@"{
	""Family"": [
		{
			""Member"": {
				""ID"": ""1"",
				""FName"": ""Paul"",
				""LName"": ""Smith"",
				""DOB"": ""1/12/1988""
			}
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
		}
	],
	""Member"": {
		""ID"": ""3"",
		""FName"": ""Joseph-ignore"",
		""LName"": ""Doe"",
		""DOB"": ""11/6/1994""
	}
}",
             //settings:
             "ClusterNode|Family,RecordNode|Member",
             //TODO: Verify that the following settings work here as well: "CollectionNode|Family,ClusterNode|,RecordNode|Member"
             //expected:
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
, 2                            )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2d
#endregion Members_2d.json


#region Members_2d1.json:
         //This test respects clusters (ClusterNode setting is present, but CollectionNode is absent) - single record is enclosed in array
         // also last record is outside of cluster node (and hence ignored)
         _testDataRepo.Add("Members_2d1", Tuple.Create(
//inputJSON:
@"{
	""Family"": [
		{
			""Member"": [ {
				""ID"": ""1"",
				""FName"": ""Paul"",
				""LName"": ""Smith"",
				""DOB"": ""1/12/1988""
			} ]
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
		}
	],
	""Member"": [ {
		""ID"": ""3"",
		""FName"": ""Joseph-ignore"",
		""LName"": ""Doe"",
		""DOB"": ""11/6/1994""
	} ]
}",
             //settings:
             "ClusterNode|Family,RecordNode|Member",
             //TODO: Verify that the following settings work here as well: "CollectionNode|Family,ClusterNode|,RecordNode|Member"
             //expected:
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
, 2                            )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2d1
#endregion Members_2d1.json


#region Members_2e.json:
         //This test respects clusters; multi-level ClusterNode
         _testDataRepo.Add("Members_2e", Tuple.Create(
//inputJSON:
@"{
  ""Members"": {
    ""Family"": [
      {
        ""X"": {
          ""Member"": {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988""
          }
        }
      },
      {
        ""X"": {
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
        ""X"": {
          ""Member"": {
            ""ID"": ""3"",
            ""FName"": ""Joseph"",
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        }
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family/X,RecordNode|Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2e
#endregion Members_2e.json


#region Members_2f.json:
         //This test respects clusters; multi-level RecordNode
         _testDataRepo.Add("Members_2f", Tuple.Create(
//inputJSON:
@"{
  ""Members"": {
    ""Family"": [
      {
        ""X"": {
          ""Member"": {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988""
          }
        }
      },
      {
        ""X"": [
          {
            ""Member"": {
              ""ID"": ""2"",
              ""FName"": ""John"",
              ""LName"": ""Green"",
              ""DOB"": ""8/23/1967""
            }
          },
          {
            ""Member"": {
              ""ID"": ""4"",
              ""FName"": ""Johnny"",
              ""LName"": ""Green"",
              ""DOB"": ""5/3/1997""
            }
          }
        ]
      },
      {
        ""X"": {
          ""Member"": {
            ""ID"": ""3"",
            ""FName"": ""Joseph"",
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        }
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|X/Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2f
#endregion Members_2f.json


#region Members_2g.json:
         //This test respects clusters; multi-level ClusterNode and RecordNode
         _testDataRepo.Add("Members_2g", Tuple.Create(
//inputJSON:
@"{
  ""Members"": {
    ""X"": [
      {
        ""Family"": {
          ""A"": {
            ""X"": {
              ""B"": {
                ""Member"": {
                  ""ID"": ""1"",
                  ""FName"": ""Paul"",
                  ""LName"": ""Smith"",
                  ""DOB"": ""1/12/1988""
                }
              }
            }
          }
        }
      },
      {
        ""Family"": {
          ""A"": {
            ""X"": [
              {
                ""B"": {
                  ""Member"": {
                    ""ID"": ""2"",
                    ""FName"": ""John"",
                    ""LName"": ""Green"",
                    ""DOB"": ""8/23/1967""
                  }
                }
              },
              {
                ""B"": {
                  ""Member"": {
                    ""ID"": ""4"",
                    ""FName"": ""Johnny"",
                    ""LName"": ""Green"",
                    ""DOB"": ""5/3/1997""
                  }
                }
              }
            ]
          }
        }
      },
      {
        ""Family"": {
          ""A"": {
            ""X"": {
              ""B"": {
                ""Member"": {
                  ""ID"": ""3"",
                  ""FName"": ""Joseph"",
                  ""LName"": ""Doe"",
                  ""DOB"": ""11/6/1994""
                }
              }
            }
          }
        }
      }
    ]
  }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|X/Family/A,RecordNode|X/B/Member",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2g
         #endregion Members_2g.json


#region Members_2h.json:
         //This test respects clusters; implied collection and cluster nodes
         _testDataRepo.Add("Members_2h", Tuple.Create(
//inputJSON:
@"[
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
]",
             //settings:
             "ClusterNode|,RecordNode|Member",  //here, CollectionNode must be null and ClusterNode must be empty
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2h
         #endregion Members_2h.json


#region Members_2i.json:
         //This test respects clusters; implied collection, cluster and record nodes
         _testDataRepo.Add("Members_2i", Tuple.Create(
//inputJSON:
@"[
   [
      {
         ""ID"": ""1"",
         ""FName"": ""Paul"",
         ""LName"": ""Smith"",
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
         ""DOB"": ""11/6/1994""
      }
   ]
]",
             //settings:
             "ClusterNode|,RecordNode|",  //here, CollectionNode must be null while ClusterNode and RecordNode must be empty
                                          //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2i
         #endregion Members_2i.json


#region Members_2j.json:
         //This test respects clusters; implied collection and record nodes (but no cluster node)
         _testDataRepo.Add("Members_2j", Tuple.Create(
//inputJSON:
@"{
   ""Family"": [
      [
         {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
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
            ""DOB"": ""11/6/1994""
         }
      ]
   ]
}",
             //settings:
             "ClusterNode|Family,RecordNode|",  //here, CollectionNode must be null while ClusterNode and RecordNode must be empty
                                                //TODO: Verify that the following settings work here as well: "CollectionNode|Family,ClusterNode|,RecordNode|"
                                                //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2j
#endregion Members_2j.json


#region Members_2k.json:
         //This test respects clusters; implied record nodes (but no collection or cluster node)
         _testDataRepo.Add("Members_2k", Tuple.Create(
//inputJSON:
@"{
   ""Distractor"" : { ""Family"" : [ [ { ""blah"" : ""blah"" } ] ] },
   ""Members"" :
   {
      ""Family"": [
         [
            {
               ""ID"": ""1"",
               ""FName"": ""Paul"",
               ""LName"": ""Smith"",
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
               ""DOB"": ""11/6/1994""
            }
         ]
      ]
   }
}",
             //settings:
             "CollectionNode|Members,ClusterNode|Family,RecordNode|",
             //expected:
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
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_2k
#endregion Members_2k.json


#region Members_4.json:
         //This test demonstrates dot notation of naming nested items
         _testDataRepo.Add("Members_4", Tuple.Create(
//inputJSON:
@"{
	""Members"": {
		""Member"": [
			{
				""ID"": ""1"",
				""Name"": {
					""First"":	{
						""Suffix"": ""Jr"",
						""__text"": ""Paul Mike""
					},
					""Last"": ""Smith"",
					""__text"": ""Jones-Junior""
				},
				""DOB"": ""1/12/1988"",
				""Empty"":	""""
			},
			{
				""ID"": ""2"",
				""Name"": {
					""First"":	""John"",
					""Last"": ""Green""
				},
				""Empty"":	"""",
				""DOB"": ""8/23/1967"",
				""__explicitText__"": ""Explicit record text""
			},
			{
				""ID"": ""3"",
				""Name"": ""Donald Duck""
			},
			{
				""ID"": ""4"",
				""Name"": {
					""First"":	""Joseph"",
					""Last"": ""Doe""
				},
				""DOB"": ""11/6/1994"",
				""__explicitText__"": ""Another explicit text""
			}
		],
		""Dummy"":	""Non-element node to be ignored""
	}
}",
             //settings:
             "CollectionNode|Members,RecordNode|Member",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("Name.First.Suffix","Jr" as object),
                                                               Tuple.Create("Name.First.__text","Paul Mike" as object),
                                                               Tuple.Create("Name.Last","Smith" as object),
                                                               Tuple.Create("Name.__text","Jones-Junior" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty", string.Empty as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("Name.First","John" as object),
                                                               Tuple.Create("Name.Last","Green" as object),
                                                               Tuple.Create("Empty",string.Empty as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object),
                                                               Tuple.Create("__explicitText__", "Explicit record text" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","3" as object),
                                                               Tuple.Create("Name","Donald Duck" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","4" as object),
                                                               Tuple.Create("Name.First","Joseph" as object),
                                                               Tuple.Create("Name.Last","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object),
                                                               Tuple.Create("__explicitText__", "Another explicit text" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_4
#endregion Members_4.json


#region Members_5.json:
         //empty/null/array/typed items
         _testDataRepo.Add("Members_5", Tuple.Create(
//inputJSON:
@"{
	""Members"": {
		""Member"": [
			{
				""ID"": 1,
				""FName"": ""Paul"",
				""LName"": ""Smith"",
				""DOB"": ""1/12/1988"",
				""SomeElems"": [
					""a"",
					""b"",
					""c""
				],
				""EmptyElems"": [
					"""",
					""""
				],
				""EmptyElem"": """",
				""NullElem"": null,
				""BoolElem"": true,
				""Mixed"": ""normalbold""
			},
			{
				""ID"": ""2"",
				""FName"": ""John"",
				""LName"": ""Green"",
				""DOB"": ""8/23/1967""
			}
		]
	}
}",
             //settings:
             "CollectionNode|Members,RecordNode|Member",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID",1L as object),  //JSON.NET parses integer numbers into longs
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("SomeElems", "a" as object),  //arrays result in duplicate fields (to be handled in KeyValRecord ctor)
                                                               Tuple.Create("SomeElems", "b" as object),
                                                               Tuple.Create("SomeElems", "c" as object),
                                                               Tuple.Create("EmptyElems", string.Empty as object),
                                                               Tuple.Create("EmptyElems", string.Empty as object),
                                                               Tuple.Create("EmptyElem", string.Empty as object),
                                                               Tuple.Create<string,object>("NullElem", null),
                                                               Tuple.Create("BoolElem", true as object),
                                                               Tuple.Create("Mixed", "normalbold" as object)
                                                             }
                           ),
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","2" as object),
                                                               Tuple.Create("FName","John" as object),
                                                               Tuple.Create("LName","Green" as object),
                                                               Tuple.Create("DOB", "8/23/1967" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_5
#endregion Members_5.json


#region Members_6.json:
         //multiple objects (SupportMultipleContent=true needed), no commas
         _testDataRepo.Add("Members_6", Tuple.Create(
//inputJSON:
@"{
  ""ID"": ""1"",
  ""FName"": ""Paul"",
  ""LName"": ""Smith"",
  ""DOB"": ""1/12/1988"",
  ""Empty1"": """",
  ""Empty2"": """"
}
{
  ""ID"": ""2"",
  ""FName"": ""John"",
  ""LName"": ""Green"",
  ""DOB"": ""8/23/1967""
}
{
  ""ID"": ""3"",
  ""FName"": [
    ""Joseph"",
    ""Dup'd Joseph""
  ],
  ""LName"": ""Doe"",
  ""DOB"": ""11/6/1994""
}",
             //settings:
             "RecordNode|",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty1", "" as object),
                                                               Tuple.Create("Empty2", "" as object)
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
                                                               Tuple.Create("FName","Dup'd Joseph" as object),  //note that ActionOnDuplicateKey does not take effect until KeyValRecord ctor
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_6
#endregion Members_6.json


#region Members_6a.json:
         //multiple objects (SupportMultipleContent=true needed), comma separated
         _testDataRepo.Add("Members_6a", Tuple.Create(
//inputJSON:
@"{
  ""ID"": ""1"",
  ""FName"": ""Paul"",
  ""LName"": ""Smith"",
  ""DOB"": ""1/12/1988"",
  ""Empty1"": """",
  ""Empty2"": """"
},
{
  ""ID"": ""2"",
  ""FName"": ""John"",
  ""LName"": ""Green"",
  ""DOB"": ""8/23/1967""
},
{
  ""ID"": ""3"",
  ""FName"": [
    ""Joseph"",
    ""Dup'd Joseph""
  ],
  ""LName"": ""Doe"",
  ""DOB"": ""11/6/1994""
}",
             //settings:
             "RecordNode|",
             //expected:
             new List<Xrecord>
             {
                new Xrecord(new List<Tuple<string,object>>() { Tuple.Create("ID","1" as object),
                                                               Tuple.Create("FName","Paul" as object),
                                                               Tuple.Create("LName","Smith" as object),
                                                               Tuple.Create("DOB", "1/12/1988" as object),
                                                               Tuple.Create("Empty1", "" as object),
                                                               Tuple.Create("Empty2", "" as object)
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
                                                               Tuple.Create("FName","Dup'd Joseph" as object),  //note that ActionOnDuplicateKey does not take effect until KeyValRecord ctor
                                                               Tuple.Create("LName","Doe" as object),
                                                               Tuple.Create("DOB", "11/6/1994" as object)
                                                             }
                           )
             }  //expected
                                                    ) //Tuple.Create
                          );  //Add Members_6a
#endregion Members_6a.json

      }  //Initialize


      [DataTestMethod]
      [DataRow("Members_1")]
      [DataRow("Members_1a")]
      [DataRow("Members_1c")]
      [DataRow("Members_1d")]
      [DataRow("Members_1e")]
      [DataRow("Members_1f")]
      [DataRow("Members_1g")]
      [DataRow("Members_2")]
      [DataRow("Members_2b")]
      [DataRow("Members_2c")]
      [DataRow("Members_2d")]
      [DataRow("Members_2d1")]
      [DataRow("Members_2e")]
      [DataRow("Members_2f")]
      [DataRow("Members_2g")]
      [DataRow("Members_2h")]
      [DataRow("Members_2i")]
      [DataRow("Members_2j")]
      [DataRow("Members_2k")]
      [DataRow("Members_4")]
      [DataRow("Members_5")]
      [DataRow("Members_6")]
      [DataRow("Members_6a")]
      public void JsonParsing_EndToEnd_CorrectData(string testCase)
      {
         //This is a series of end-to-end integration tests of JSON parsing

         //arrange
         var testData = _testDataRepo[testCase];
         var inputJSON = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 6;
         var xrecordSupplier = new JsonFeederForSource(new StringReader(inputJSON), dummySourceNo, settings, false);
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


      [TestMethod]
      public void ReadToEnd__ConsumesAllInput()
      {
         //This end-to-end integration test of JSON parsing verifies that EOD mark is supplied upon exhausting data on input

         //arrange
         var testData = _testDataRepo["Members_1"];
         var inputJSON = testData.Item1;
         var settings = testData.Item2;
         var dummySourceNo = 6;
         var xrecordSupplier = new JsonFeederForSource(new StringReader(inputJSON), dummySourceNo, settings, false);
         var xrecordSupplierPO = new PrivateObject(xrecordSupplier);
         var actual = new List<Xrecord>();
         var expected = testData.Item3;

         //act
         xrecordSupplier.ReadToEnd();
         var elem = xrecordSupplierPO.Invoke("SupplyNextXrecord");

            //assert
            elem.Should().BeNull();
      }

   }
}
