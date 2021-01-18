//OutputProvider_tests.cs
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
using Mavidian.DataConveyer.Orchestrators;
using Mavidian.DataConveyer.Output;
using Xunit;

namespace DataConveyer.Tests.Intake
{
   public class OutputProvider_tests
   {

      [Fact]
      public void CreateProvider_Raw_OfRawType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Raw };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<RawOutputProvider>();
      }


      [Fact]
      public void CreateProvider_Keyword_OfKwType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Keyword };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<KwOutputProvider>();
      }


      [Fact]
      public void CreateProvider_Delimited_OfDelimitedType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Delimited };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<DelimitedOutputProvider>();
      }


      [Fact]
      public void CreateProvider_Flat_OfFlatType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Flat };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<FlatOutputProvider>();
      }


      [Fact]
      public void CreateProvider_Arbitrary_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Arbitrary };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<ArbitraryOutputProvider>();
      }


      [Fact]
      public void CreateProvider_XML_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.XML };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<XrecordOutputProvider>();
      }


      [Fact]
      public void CreateProvider_JSON_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.JSON };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<XrecordOutputProvider>();
      }


      [Fact]
      public void CreateProvider_UnboudJSON_OfXrecordType()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.UnboundJSON };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<XrecordOutputProvider>();
      }


      [Fact]
      public void CreateProvider_X12_OfX12Type()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.X12 };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeOfType<X12OutputProvider>();
      }


      [Fact]
      public void CreateProvider_HL7_TBD()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.HL7 };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeNull();  // future use
      }


      [Fact]
      public void CreateProvider_Ultimate_TBD()
      {
         //arrange
         var config = new OrchestratorConfig { OutputDataKind = KindOfTextData.Ultimate };

         //act
         var sut = OutputProvider.CreateProvider(config, null, null);

         //assert
         sut.Should().BeNull();  // future use
      }

   }
}
