## Project #0 - C++ Primer

> **前言**：CMU15-445的project会循序渐进地实现一个**面向磁盘**的传统**关系型**数据库 Bustub中的部分关键组件，而由于Bustub是在C++17下实现的数据库，所以设计了C++Primer这个lab来检测coder对于modern C++的熟悉程度，同时锻炼coder的debug能力。（笔者选择了**gdb**的调试方式）

### Environment

1. 框架代码：github上开源的[bustub](https://github.com/cmu-db/bustub)的框架代码。
2. 使用工具：Linux系统 + vscode + gdb + clang + CMake
3. 代码规范： [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) (C++代码风格) + [LLVM Address Sanitizer (ASAN) and Leak Sanitizer (LSAN)](https://clang.llvm.org/docs/AddressSanitizer.html) (检测是否有内存泄漏)

### Requirement

*(ps : 详情请见[官网](https://15445.courses.cs.cmu.edu/fall2023/project0/))*

1. Data Structure : Trie
2. Role : Key-Value Store
3. Operations : CRUD(增删查改)
4. Improve Performance : 高并发(可支持多个readers和单个writer同时对trie做查询/修改)、Copy-On-Write

### Great Idea

- **[MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)的思想来实现高并发**
  - in-place CRUD的缺陷：在传统的前缀树中，增删查改是in-place的，要实现安全的并发，需要运用读写锁，而reader和writer必然会产生race condition。这种情况下仅支持多个readers**或**单个writer同时对trie做查询/修改，并发性能低。
  - MVCC的优势：MVCC是通过保留多个数据项的副本的思想来实现高并发的。特别的，对于KV-Store的前缀树，其通过在更新操作时（Put/Remove）保留old trie的方式来实现“保留副本”，从而支持了多个readers（在old trie上操作）**和**单个writer（在new trie上操作）的并发。
- **[Copy-On-Write](https://en.wikipedia.org/wiki/Copy-on-write)的方式来实现高性能**
  - COW(Copy-On-Write)是编程上对于资源管理的优化技巧，通过在需要拷贝时先进行隐式拷贝（主要通过指针实现“sharing”），在有write相关操作作用于共享的资源时，再进行真正拷贝。
  - COW在某些情形下有助于解决过多无效拷贝所带来的性能问题，例如copy-on-write的`fork()`。（在我的xv6实验中有实现）。
  - 特别的，对于KV-Store的前缀树，由于我们需要保留old trie的副本，故可以通过copy-on-write避免某些结点的无效拷贝。

### 面向对象和泛型编程

（ps：这两个C++的重要特性在该project中运用的不多，日后另找资源补充……）

- cow trie的实现中涉及了三个类——基类TrieNode、派生类TrieNodeWithValue、以及Trie。
  - TrieNode部分函数为虚函数(virtual function)，虚函数提供了运行时多态性（基类可以调用派生类中的覆写函数），使派生类可以override基类中的相关函数；特别的，纯虚函数可运用于定义抽象类的接口。

- 由于TrieNode的value可以是各种类型的，implementation中在value处运用到了泛型编程的技巧，提供了一个高层级的对变量类型的抽象。

### Modern C++ Feature

1. `auto`：用于编译时自动的类型推导，方便了开发者也使代码更加简洁。例如：`for(auto& ch : string){}`

2. `const`：const在该项目中保证了old trie中的结点不变。

3. `shared_ptr` & `unique_ptr`： 二者均属于智能指针，用于优化C/C++中内存泄漏的问题。可通过`std::move`实现后者到前者的转化。

4. `std::move`：移动语义，能高效地完成资源归属的转移。（同时也是左值到右值的转化）

5. `dynamic_cast`：能够确保目标指针类型所指向的是一个有效且完整的对象，允许upcast和downcast。更详细的C++cast的用法可见[该文章](https://zhuanlan.zhihu.com/p/151744661)。

6. `std::mutex`：C++标准库中实现的互斥锁，用于并发控制。相关API为`lock()`、`unlock`

   ……

### Implementation

***Task #1 - Copy-On-Write Trie***

- `Trie::Get(std::string_view key)`
- 根据所给的key，在trie中寻找对应的value，若没有则返回nullptr。

```c++
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    auto const value_trie = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
    if (value_trie && root_->is_value_node_) {
      return value_trie->value_.get();
    }
    return nullptr;
  }
  std::shared_ptr<const TrieNode> curr = root_;
  for (auto it = key.begin(); it != key.end(); ++it) {
    auto iter = curr->children_.find(*it);
    if (iter == curr->children_.end()) {
      return nullptr;
    }
    if ((it + 1) == key.end()) {
      curr = iter->second;
      auto const value_trie = dynamic_cast<const TrieNodeWithValue<T> *>(curr.get());
      if (value_trie && curr->is_value_node_) {
        return value_trie->value_.get();
      }
      return nullptr;
    }
    curr = iter->second;
  }
  return nullptr;
}

```



- `Trie::Put(std::string_view key, T value)`
- 在Trie中插入一个KV值，若已存在则更新old value为new value。

```c++
template <class T>

auto Trie::Put(std::string_view key, T value) const -> Trie {

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.



  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already

  // exists, you should create a new `TrieNodeWithValue`.

  std::unique_ptr<TrieNode> tmp_root = (root_ == nullptr) ? std::make_unique<TrieNode>() : root_->Clone();

  std::shared_ptr<TrieNode> new_root = std::move(tmp_root);

  auto curr = new_root;

  if (key.empty()) {

​    std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));

​    auto helper = std::make_shared<TrieNodeWithValue<T>>(curr->children_, value_ptr);

​    new_root = std::move(helper);

  } else {

​    for (auto it = key.begin(); it != key.end(); ++it) {

​      auto iter = curr->children_.find(*it);

​      if (iter != curr->children_.end()) {
.
​        auto tmp = iter->second;

​        auto tmp_node = tmp->Clone();

​        std::shared_ptr<TrieNode> new_node = std::move(tmp_node);

​        curr->children_.erase(iter->first);

​        if (it == key.end() - 1) {

​          std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));

​          auto newnode_with_value = std::make_shared<TrieNodeWithValue<T>>(new_node->children_, value_ptr);

​          std::shared_ptr<const TrieNode> helper = std::move(newnode_with_value);

​          curr->children_.insert(std::make_pair(*it, std::move(helper)));

​          break;

​        }

​        curr->children_.insert(std::make_pair(*it, new_node));

​        curr = new_node;

​      } else {

​        auto tmp_node = curr->Clone();

​        tmp_node->children_.clear();

​        if (it == key.end() - 1) {

​          std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));

​          auto newnode_with_value = std::make_shared<const TrieNodeWithValue<T>>(value_ptr);

​          std::shared_ptr<const TrieNode> helper = std::move(newnode_with_value);

​          curr->children_.insert(std::make_pair(*it, std::move(helper)));

​          break;

​        }

​        std::shared_ptr<TrieNode> new_node = std::move(tmp_node);

​        curr->children_.insert(std::make_pair(*it, new_node));

​        curr = new_node;

​      }

​    }

  }



  auto new_trie = Trie(new_root);

  return new_trie;

}.
```



- `Trie::Remove(std::string_view key)`
- 在Trie中移除key值结点对应的value，若Trie中结点无value有children，类型转化；若无value无children，删除结点。
- ADT：stack

```C++
auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::unique_ptr<TrieNode> tmp_root = (root_ == nullptr) ? std::make_unique<TrieNode>() : root_->Clone();
  tmp_root->is_value_node_ = root_->is_value_node_;
  std::shared_ptr<TrieNode> new_root = std::move(tmp_root);
  std::shared_ptr<const TrieNode> curr = new_root;
  std::stack<std::shared_ptr<TrieNode>> node_stack;
  std::stack<char> ch_stack;
  node_stack.push(new_root);
  // The loop for push operation.
  for (auto ch : key) {
    auto iter = curr->children_.find(ch);
    // Failed to find the key in the trie.
    if (iter == curr->children_.end()) {
      return Trie(root_);
    }
    curr = iter->second;
    auto tmp_node = curr->Clone();
    tmp_node->is_value_node_ = curr->is_value_node_;
    std::shared_ptr<TrieNode> new_node = std::move(tmp_nod.e);
    auto top_node = node_stack.top();
    top_node->children_.erase(iter->first);
    top_node->children_.insert(std::make_pair(ch, new_node));
    ch_stack.push(ch);
    node_stack.push(new_node);
  }

  auto target_node = node_stack.top();
  target_node->is_value_node_ = false;

  // The loop for pop operation.(construct the trie down-up)
  while (!node_stack.empty()) {
    auto top_node = node_stack.top();
    node_stack.pop();
    if (node_stack.empty()) {
      if (top_node->children_.empty() && !top_node->is_value_node_) {
        new_root = nullptr;
      }
      break;
    }
    auto ch_helper = ch_stack.top();
    ch_stack.pop();
    auto helper = node_stack.top();
    if (top_node->children_.empty() && !top_node->is_value_node_) {
      helper->children_.erase(ch_helper);
    }
  }
  auto new_trie = Trie(new_root);
  return new_trie;
}
```



***Task #2 - Concurrent Key-Value Store***

- `TrieStore::Get(std::string_view key)`
- 实现并发场景下的Get()函数

```c++
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {

  // Pseudo-code:

  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the

  //     trie while holding the root lock.

  root_lock_.lock();

  auto saved_root = root_;

  root_lock_.unlock();

  // (2) Lookup the value in the trie.

  auto value_ptr = saved_root.Get<T>(key);

  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the

  //     root. Otherwise, return std::nullopt.

  if (value_ptr) {

​    return ValueGuard<T>(saved_root, *value_ptr);

  }

  return std::nullopt;

}
```



- `TrieStore::Put(std::string_view key, T value)`
- 实现并发场景下的Put()函数

```c++
template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  write_lock_.lock();
  auto tmp_root = root_.Put(key, std::move(value));  // 为什么这里的value要执行移动语义？
  root_lock_.lock();
  root_ = tmp_root;
  root_lock_.unlock();
  write_lock_.unlock();
}
```



- `TrieStore::Remove(std::string_view key)`
- 实现并发场景下的Remove()函数

```c++
void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  write_lock_.lock();
  auto tmp_root = root_.Remove(key);
  root_lock_.lock();
  root_ = tmp_root;
  root_lock_.unlock();
  write_lock_.unlock();
}
```

### Debug

**BUG**

1. Failed to dynamic_cast from TrieNode to TrieNodeWithValue.(Solution:Avoid dynamic_cast here.)

2. Get()返回值错误，由于定位错了导致bug的代码段（以为在Put()函数中），一直以为是cast的问题，消耗了很多时间才发现是Get()函数的代码段出了问题。当然，即使是很弱智的bug，在debug过程中得到的“附属品”也是很有价值的，而且在debug的过程中也加深了对于代码细节的理解。

3. 未考虑key.empty()为true的情况（ps：leetcode题中经常遇到这种bug，感觉通过加强算法的“统一性”从而减少特殊情形的复杂度挺重要的，在Remove()函数中可以完成这种统一）。

4. 将TrieNodeWithValue转化为TrieNode的方法有缺陷（仅改变is_value_node），而Clone()方法的内部逻辑考虑到了value_依然存在而将is_value_node赋值为true。我深入到Clone()的具体细节才发现了该bug出现的原因，这是由于函数抽象了实现的细节导致coder理解偏差导致的。这个bug我觉得比较好的解决方案我无法用代码写出来，于是用了一个比较“丑陋”的方法，有点改变了Clone()函数的本意。（我感觉难点在于如何将TrieNodeWithValue转化为TrieNode）

5. C++真挺难的……

**Debug**

- 我个人暂时比较钟意的debug方式是**gdb+printf()/std::cout**。

### Gradescope

