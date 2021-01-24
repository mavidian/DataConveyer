//TextDispenserForTarget.cs
//
// Copyright © 2017-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


using System.IO;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   ///  Text writer to a single target.
   /// </summary>
   internal class TextDispenserForTarget : LineDispenserForTarget
   {
      /// <summary>
      /// Creates an instance of a text line dispenser for a single target.
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo">1 based target number.</param>
      internal TextDispenserForTarget(TextWriter writer, int targetNo) : base(writer, targetNo) { }

      //Note that SendNextLine, SendNextLineAsync, ... are not overridden in this class; so, effectively
      // TextDispenserForTarget functionality is the same as of the base class, i.e. LineDispenserForTarget

   }
}
