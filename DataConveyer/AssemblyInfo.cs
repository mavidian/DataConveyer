//AssemblyInfo.cs
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


using System;
using System.Runtime.CompilerServices;


// Note that AssemblyInfo.cs files are no longer generated for .NET Standard projects
// (as most assembly attributes are placed in the .csproj file).
// However, some attributes are not in the .csproj file, hence this file was manually added.
// Also note that some attributes can be manually added to .csproj file, e.g.:
//
//  <ItemGroup Condition="'$(Configuration)'=='Debug'">
//    <AssemblyAttribute Include = "System.Runtime.CompilerServices.InternalsVisibleTo" >
//      < _Parameter1 >$(AssemblyName) _tests</_Parameter1>
//    </AssemblyAttribute>
// </ItemGroup>
//
// But this would only work in case of string parameter. So, it can't be done for
// CLSCompliant attributes, which takes argument of type bool.
// See https://github.com/Microsoft/msbuild/issues/2281


[assembly: CLSCompliant(true)]
[assembly: InternalsVisibleTo("DataConveyer_tests, PublicKey=002400000480000094000000060200000024000052534131000400000100010035aba33a18362e" +
                                                            "fd0a53a68db1a8e3f08ec1a9b3e1f29ea3b5a7f003a2284d12528ad84ce1d78d24b57f8f9d2213" +
                                                            "f968db1dd76eaa0fcd1d24d5f3cb32a1386a7d94fa653c912ca13e518d201ae2560193cf09dae7" +
                                                            "10368fd320e6d4be7ae38b07f6e4133eef26228723215060aa804f7a509fc9f9f1f6b0802a8231" +
                                                            "2623e5c9")]
[assembly: InternalsVisibleTo("DataConveyer.Tests, PublicKey=002400000480000094000000060200000024000052534131000400000100010035aba33a18362e" +
                                                            "fd0a53a68db1a8e3f08ec1a9b3e1f29ea3b5a7f003a2284d12528ad84ce1d78d24b57f8f9d2213" +
                                                            "f968db1dd76eaa0fcd1d24d5f3cb32a1386a7d94fa653c912ca13e518d201ae2560193cf09dae7" +
                                                            "10368fd320e6d4be7ae38b07f6e4133eef26228723215060aa804f7a509fc9f9f1f6b0802a8231" +
                                                            "2623e5c9")]
