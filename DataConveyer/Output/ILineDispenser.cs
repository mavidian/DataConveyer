//ILineDispenser.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Represents sequential input to text target(s) - synchronous and asynchronous.
   /// </summary>
   internal interface ILineDispenser : IDisposable
   {
      /// <summary>
      /// Write a line to the appropriate target.
      /// A line represents a single record and is generally separated by CRLF, except for
      /// data kinds such as X12, XML or JSON.
      /// </summary>
      /// <param name="linePlus">Next line (along with target number) or null at end.</param>
      void SendNextLine(Tuple<ExternalLine, int> linePlus);
      /// <summary>
      /// Asynchronously write a line to the appropriate target.
      /// A line represents a single record and is generally separated by CRLF, except for
      /// data kinds such as X12, XML or JSON.
      /// </summary>
      /// <param name="linePlus"></param>
      /// <returns></returns>
      Task SendNextLineAsync(Tuple<ExternalLine, int> linePlus);
   }
}
