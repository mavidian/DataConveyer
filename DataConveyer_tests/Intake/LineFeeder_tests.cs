//LineFeeder_tests.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.IO;
using System.Linq;
using System.Text;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class LineFeeder_tests
   {
      private const string FirstSource = "First of First\r\nSecond of First\r\nThird of First";
      private const string SecondSource = "First of Second\r\nSecond of Second";
      private const string ThirdSource = "First of Third\r\nSecond of Third\r\nThird of Third\r\nFourth of Third";

      private MemoryStream _firstStream;
      private MemoryStream _secondStream;
      private MemoryStream _thirdStream;


      [TestInitialize()]
      public void Initialize()
      {
         _firstStream = new MemoryStream(Encoding.UTF8.GetBytes(FirstSource));
         _secondStream = new MemoryStream(Encoding.UTF8.GetBytes(SecondSource));
         _thirdStream = new MemoryStream(Encoding.UTF8.GetBytes(ThirdSource));
      }


      // Tests for GetNextLine method.


      [TestMethod]
      public void GetNextLine_ThreeSources_CorrectDataReturned()
      {
         //arrange
         var streams = new MemoryStream[] { _firstStream, _secondStream, _thirdStream };
         var feeder = LineFeederCreator.CreateLineFeeder(streams.Select(s => new StreamReader(s, Encoding.UTF8)), KindOfTextData.Raw, false, false, null, null);

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();
         var fourth = feeder.GetNextLine();
         var fifth = feeder.GetNextLine();
         var sixth = feeder.GetNextLine();
         var seventh = feeder.GetNextLine();
         var eighth = feeder.GetNextLine();
         var ninth = feeder.GetNextLine();
         var tenth = feeder.GetNextLine();
         var eleventh = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First of First");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second of First");
         second.Item2.Should().Be(1);
         third.Item1.Text.Should().Be("Third of First");
         third.Item2.Should().Be(1);
         fourth.Item1.Text.Should().Be("First of Second");
         fourth.Item2.Should().Be(2);
         fifth.Item1.Text.Should().Be("Second of Second");
         fifth.Item2.Should().Be(2);
         sixth.Item1.Text.Should().Be("First of Third");
         sixth.Item2.Should().Be(3);
         seventh.Item1.Text.Should().Be("Second of Third");
         seventh.Item2.Should().Be(3);
         eighth.Item1.Text.Should().Be("Third of Third");
         eighth.Item2.Should().Be(3);
         ninth.Item1.Text.Should().Be("Fourth of Third");
         ninth.Item2.Should().Be(3);
         tenth.Should().BeNull();
         eleventh.Should().BeNull();
      }

      [TestMethod]
      public void GetNextLine_ThreeSourcesSkipRepeatedHeaders_RepeatedHeadersSkipped()
      {
         //arrange
         var streams = new MemoryStream[] { _firstStream, _secondStream, _thirdStream };
         var feeder = LineFeederCreator.CreateLineFeeder(streams.Select(s => new StreamReader(s, Encoding.UTF8)), KindOfTextData.Raw, false, true, null, null);

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();
         var fourth = feeder.GetNextLine();
         var fifth = feeder.GetNextLine();
         var sixth = feeder.GetNextLine();
         var seventh = feeder.GetNextLine();
         var eighth = feeder.GetNextLine();
         var ninth = feeder.GetNextLine();
         var tenth = feeder.GetNextLine();
         var eleventh = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First of First");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second of First");
         second.Item2.Should().Be(1);
         third.Item1.Text.Should().Be("Third of First");
         third.Item2.Should().Be(1);
         fourth.Item1.Text.Should().Be("Second of Second");
         fourth.Item2.Should().Be(2);
         fifth.Item1.Text.Should().Be("Second of Third");
         fifth.Item2.Should().Be(3);
         sixth.Item1.Text.Should().Be("Third of Third");
         sixth.Item2.Should().Be(3);
         seventh.Item1.Text.Should().Be("Fourth of Third");
         seventh.Item2.Should().Be(3);
         eighth.Should().BeNull();
         ninth.Should().BeNull();
         tenth.Should().BeNull();
         eleventh.Should().BeNull();
      }


      // Tests for GetNextLineAsync method.
      // Note that at end of sequence LineFeeder returns a null tuple;
      // (which follows the tuple containing null (not null tuple) at end of the last sequence returned by LineFeederForSource).


      [TestMethod]
      public void GetNextLineAsync_ThreeSources_CorrectDataReturned()
      {
         //arrange
         var streams = new MemoryStream[] { _firstStream, _secondStream, _thirdStream };
         var feeder = LineFeederCreator.CreateLineFeeder(streams.Select(s => new StreamReader(s, Encoding.UTF8)), KindOfTextData.Raw, false, false, null, null);

         //act
         var first = feeder.GetNextLineSynced();
         var second = feeder.GetNextLineSynced();
         var third = feeder.GetNextLineSynced();
         var fourth = feeder.GetNextLineSynced();
         var fifth = feeder.GetNextLineSynced();
         var sixth = feeder.GetNextLineSynced();
         var seventh = feeder.GetNextLineSynced();
         var eighth = feeder.GetNextLineSynced();
         var ninth = feeder.GetNextLineSynced();
         var tenth = feeder.GetNextLineSynced();
         var eleventh = feeder.GetNextLineSynced();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First of First");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second of First");
         second.Item2.Should().Be(1);
         third.Item1.Text.Should().Be("Third of First");
         third.Item2.Should().Be(1);
         fourth.Item1.Text.Should().Be("First of Second");
         fourth.Item2.Should().Be(2);
         fifth.Item1.Text.Should().Be("Second of Second");
         fifth.Item2.Should().Be(2);
         sixth.Item1.Text.Should().Be("First of Third");
         sixth.Item2.Should().Be(3);
         seventh.Item1.Text.Should().Be("Second of Third");
         seventh.Item2.Should().Be(3);
         eighth.Item1.Text.Should().Be("Third of Third");
         eighth.Item2.Should().Be(3);
         ninth.Item1.Text.Should().Be("Fourth of Third");
         ninth.Item2.Should().Be(3);
         tenth.Should().BeNull();
         eleventh.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLineAsync_ThreeSourcesSkipRepeatedHeaders_RepeatedHeadersSkipped()
      {
         //arrange
         var streams = new MemoryStream[] { _firstStream, _secondStream, _thirdStream };
         var feeder = LineFeederCreator.CreateLineFeeder(streams.Select(s => new StreamReader(s, Encoding.UTF8)), KindOfTextData.Raw, false, true, null, null);

         //act
         var first = feeder.GetNextLineSynced();
         var second = feeder.GetNextLineSynced();
         var third = feeder.GetNextLineSynced();
         var fourth = feeder.GetNextLineSynced();
         var fifth = feeder.GetNextLineSynced();
         var sixth = feeder.GetNextLineSynced();
         var seventh = feeder.GetNextLineSynced();
         var eighth = feeder.GetNextLineSynced();
         var ninth = feeder.GetNextLineSynced();
         var tenth = feeder.GetNextLineSynced();
         var eleventh = feeder.GetNextLineSynced();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First of First");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second of First");
         second.Item2.Should().Be(1);
         third.Item1.Text.Should().Be("Third of First");
         third.Item2.Should().Be(1);
         fourth.Item1.Text.Should().Be("Second of Second");
         fourth.Item2.Should().Be(2);
         fifth.Item1.Text.Should().Be("Second of Third");
         fifth.Item2.Should().Be(3);
         sixth.Item1.Text.Should().Be("Third of Third");
         sixth.Item2.Should().Be(3);
         seventh.Item1.Text.Should().Be("Fourth of Third");
         seventh.Item2.Should().Be(3);
         eighth.Should().BeNull();
         ninth.Should().BeNull();
         tenth.Should().BeNull();
         eleventh.Should().BeNull();
      }

   }
}