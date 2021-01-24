//MultipleSources_tests.cs
//
// Copyright © 2019-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System;
using System.Collections.Generic;
using Xunit;

namespace DataConveyer.Tests.Common
{
   public class ExternalLine_tests
   {

      [Fact]
      public void Ctor_Xtext_CorrectData()
      {
         //arrange, act
         ExternalLine sut = new Xtext("blahblah");

         //assert
         sut.Should().BeOfType<Xtext>();
         sut.Type.Should().Be(ExternalLineType.Xtext);
         sut.Excerpt.Should().Be("blahblah");
         sut.ClstrNo.Should().Be(0);
         sut.Text.Should().Be("blahblah");
         sut.Items.Should().BeNull();
      }


      [Fact]
      public void CreateXtext_PassNull_ReceiveNull()
      {
         //arrange, act
         ExternalLine sut = ExternalLine.CreateXtext(null);

         //assert
         sut.Should().BeNull();
      }


      [Fact]
      public void Ctor_Xrecord_CorrectData()
      {
         //arrange, act
         ExternalLine sut = new Xrecord(new List<Tuple<string, object>>() { Tuple.Create("fld 1",(object)"blah1"),
                                                                            Tuple.Create("fld 2",(object)"blah2")
                                                                          }, 4);

         //assert
         sut.Should().BeOfType<Xrecord>();
         sut.Type.Should().Be(ExternalLineType.Xrecord);
         sut.Excerpt.Should().Be("fld 1=blah1");  //BTW, this Xrecord wouldn't be a valid XML (space in field/node name)
         sut.ClstrNo.Should().Be(4);
         sut.Text.Should().BeNull();
         sut.Items.Should().HaveCount(2);
         sut.Items[0].Should().BeOfType<Tuple<string, object>>();
         sut.Items[0].Item1.Should().Be("fld 1");
         sut.Items[0].Item2.Should().Be("blah1");
         sut.Items[1].Should().BeOfType<Tuple<string, object>>();
         sut.Items[1].Item1.Should().Be("fld 2");
         sut.Items[1].Item2.Should().Be("blah2");
      }
   }
}