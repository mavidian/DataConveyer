//PrivateAccessor.cs
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


using System;

namespace DataConveyer.Tests.TestHelpers
{
   //Inspired by https://www.teamscs.com/2017/06/unit-testing-testing-private-methods/

   /// <summary>
   /// Facilitates access to private members for testing purposes.
   /// Akin PrivateObject class (which is not available in xUnit for .NET Standard).
   /// </summary>
   internal class PrivateAccessor
   {
      private readonly object _subject;
      internal PrivateAccessor(object subject)
      {
         _subject = subject;
      }

      internal object Invoke(string methodName, params object[] args)
      {
         var methodInfo = _subject.GetType().GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
         if (methodInfo == null) throw new MissingMethodException($"Method { methodName } was not found.");
         return methodInfo.Invoke(_subject, args);
      }

      internal object GetField(string fieldName)
      {
         var fieldInfo = _subject.GetType().GetField(fieldName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
         if (fieldInfo == null) throw new MissingFieldException($"Field { fieldName } was not found.");
         return fieldInfo.GetValue(_subject);
      }
   }
}
