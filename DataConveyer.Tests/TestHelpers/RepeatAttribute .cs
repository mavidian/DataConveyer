//RepeatAttribute.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


using System.Linq;
using Xunit.Sdk;

namespace DataConveyer.Tests.TestHelpers
{
   //Based on https://stackoverflow.com/questions/31873778/xunit-test-fact-multiple-times

   /// <summary>
   /// XUnit attribute to repeat the test (Theory) a given number of times.
   /// This attribute causes the decorated method to be called with 2 int parameters: iterationNumber and repeatCount.
   /// </summary>
   public sealed class RepeatAttribute : DataAttribute
   {
      private readonly int _repeatCount;

      public RepeatAttribute(int repeatCount)
      {
         if (repeatCount < 1)
         {
            throw new System.ArgumentOutOfRangeException(
                paramName: nameof(repeatCount),
                message: "Repeat count must be greater than 0."
                );
         }
         _repeatCount = repeatCount;
      }

      public override System.Collections.Generic.IEnumerable<object[]> GetData(System.Reflection.MethodInfo testMethod)
      {
         foreach (var iterationNumber in Enumerable.Range(1, _repeatCount))
         {
            yield return new object[] { iterationNumber, _repeatCount };
         }
      }
   }
}
