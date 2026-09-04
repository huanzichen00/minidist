Client
   │
   ▼
Node
   │
   ▼
Consistent Hash Ring
   │
   ├─ physical node A
   │     ├─ vnode A#0
   │     ├─ vnode A#1
   │     └─ ...
   │
   ├─ physical node B
   │
   └─ physical node C
         │
         ▼
       owner
         │
     ┌───┴────┐
     ▼        ▼
   local     HTTP
   store     forward
