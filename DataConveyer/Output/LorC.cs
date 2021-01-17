//LorC.cs
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

using System;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Helper class for UnboundJsonDispenserForTarget.
   /// Label or Counter: "Either" type; type of elements on stack of key segments. Counter is equivalent to array index.
   /// JSON object is represented by a Label; JSON array element is represented by a Label and a Counter.
   /// </summary>
   internal class LorC : IEquatable<LorC>
   {
      private string _label;
      private int _counter;


      public LorC(string label) { _label = label; }  // creates Label
      public LorC() { _label = null; _counter = 1; }  // creates Counter (1-based)
      public LorC(int counter) { _label = null; _counter = counter; }  // creates Counter from existing segment of column name

      public bool IsCounter => _label == null;
      public bool IsLabel => !IsCounter;

      public string Label => IsLabel ? _label : string.Empty;
      public int Counter => IsCounter ? _counter : default;
      public string Value => IsLabel ? _label : $"{_counter:D2}";

      public void Increment() => _counter++;

      public bool Equals(LorC other)
      {
         if (other == null) return false;
         return Label == other.Label && Counter == other.Counter;
      }
   }
}
