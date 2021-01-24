//ILineFeeder.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Represents sequential input from text source(s) - synchronous and asynchronous.
   /// </summary>
   internal interface ILineFeeder : IDisposable
   {
      /// <summary>
      /// Return next line.
      /// A line represents a single record on intake and is generally separated by CRLF, except for
      /// data kinds such as X12, XML or JSON.
      /// </summary>
      /// <returns>Next available line (along with source number) or null at end.</returns>
      Tuple<ExternalLine, int> GetNextLine();


      /// <summary>
      /// Asynchronously return next line.
      /// A line represents a single record on intake and is generally separated by CR/LF, except for
      /// data kinds such as X12, XML or JSON.
      /// </summary>     
      /// <returns>Task with the next available line (along with source number) or null at end.</returns>
      Task<Tuple<ExternalLine, int>> GetNextLineAsync();
   }
}
