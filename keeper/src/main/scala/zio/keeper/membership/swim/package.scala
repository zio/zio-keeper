package zio.keeper.membership

import zio.Has

package object swim {
  type ConversationId = Has[ConversationId.Service]
  type Nodes          = Has[Nodes.Service]
}
