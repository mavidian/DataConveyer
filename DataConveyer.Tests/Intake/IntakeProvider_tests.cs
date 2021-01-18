//IntakeProvider_tests.cs
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
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Orchestrators;
using Xunit;

namespace DataConveyer.Tests.Intake
{
   public class IntakeProvider_tests
   {

      [Fact]
      public void CreateProvider_Raw_OfRawType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Raw };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<RawIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_Keyword_OfKwType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Keyword };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<KwIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_Delimited_OfDelimitedType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Delimited };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<DelimitedIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_Flat_OfFlatType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Flat };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<FlatIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_Arbitrary_OfArbitraryType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Arbitrary, ArbitraryInputDefs = new string[] { "dummy dummy" } };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<ArbitraryIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_XML_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.XML };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<XrecordIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_JSON_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.JSON };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<XrecordIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_UnboudJSON_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.UnboundJSON };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<XrecordIntakeProvider>();
      }


      [Fact]
      public void CreateProvider_X12_OfX12Type()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.X12 };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeOfType<X12IntakeProvider>();
      }


      [Fact]
      public void CreateProvider_HL7_TBD()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.HL7 };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeNull();  // future use
      }


      [Fact]
      public void CreateProvider_Ultimate_TBD()
      {
         //arrange
         var config = new OrchestratorConfig { InputDataKind = KindOfTextData.Ultimate };

         //act
         var sut = IntakeProvider.CreateProvider(config, null, null, null);

         //assert
         sut.Should().BeNull();  // future use
      }

   }
}
