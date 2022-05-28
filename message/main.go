// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import "github.com/datastax/go-cassandra-native-protocol/primitive"

// DefaultMessageCodecs returns a map of all the default message codecs declared in this package, keyed by their
// respective opcodes.
func DefaultMessageCodecs() map[primitive.OpCode]Codec {
	return map[primitive.OpCode]Codec{
		primitive.OpCodeAuthChallenge: &authChallengeCodec{},
		primitive.OpCodeAuthResponse:  &authResponseCodec{},
		primitive.OpCodeAuthSuccess:   &authSuccessCodec{},
		primitive.OpCodeAuthenticate:  &authenticateCodec{},
		primitive.OpCodeBatch:         &batchCodec{},
		primitive.OpCodeDseRevise:     &reviseCodec{},
		primitive.OpCodeError:         &errorCodec{},
		primitive.OpCodeEvent:         &eventCodec{},
		primitive.OpCodeExecute:       &executeCodec{},
		primitive.OpCodeOptions:       &optionsCodec{},
		primitive.OpCodePrepare:       &prepareCodec{},
		primitive.OpCodeQuery:         &queryCodec{},
		primitive.OpCodeReady:         &readyCodec{},
		primitive.OpCodeRegister:      &registerCodec{},
		primitive.OpCodeResult:        &resultCodec{},
		primitive.OpCodeStartup:       &startupCodec{},
		primitive.OpCodeSupported:     &supportedCodec{},
	}
}
