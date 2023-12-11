#include "primer/trie.h"
#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <stack>
#include <stdexcept>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

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

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::unique_ptr<TrieNode> tmp_root = (root_ == nullptr) ? std::make_unique<TrieNode>() : root_->Clone();
  std::shared_ptr<TrieNode> new_root = std::move(tmp_root);
  auto curr = new_root;
  if (key.empty()) {
    std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
    auto helper = std::make_shared<TrieNodeWithValue<T>>(curr->children_, value_ptr);
    new_root = std::move(helper);
  } else {
    for (auto it = key.begin(); it != key.end(); ++it) {
      auto iter = curr->children_.find(*it);
      if (iter != curr->children_.end()) {
        auto tmp = iter->second;
        auto tmp_node = tmp->Clone();
        std::shared_ptr<TrieNode> new_node = std::move(tmp_node);
        curr->children_.erase(iter->first);
        if (it == key.end() - 1) {
          std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
          auto newnode_with_value = std::make_shared<TrieNodeWithValue<T>>(new_node->children_, value_ptr);
          std::shared_ptr<const TrieNode> helper = std::move(newnode_with_value);
          curr->children_.insert(std::make_pair(*it, std::move(helper)));
          break;
        }
        curr->children_.insert(std::make_pair(*it, new_node));
        curr = new_node;
      } else {
        auto tmp_node = curr->Clone();
        tmp_node->children_.clear();
        if (it == key.end() - 1) {
          std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
          auto newnode_with_value = std::make_shared<const TrieNodeWithValue<T>>(value_ptr);
          std::shared_ptr<const TrieNode> helper = std::move(newnode_with_value);
          curr->children_.insert(std::make_pair(*it, std::move(helper)));
          break;
        }
        std::shared_ptr<TrieNode> new_node = std::move(tmp_node);
        curr->children_.insert(std::make_pair(*it, new_node));
        curr = new_node;
      }
    }
  }

  auto new_trie = Trie(new_root);
  return new_trie;
}

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
    std::shared_ptr<TrieNode> new_node = std::move(tmp_node);
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

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

/*
BUG:
<1>Failed to dynamic_cast from TrieNode to TrieNodeWithValue.(Solution:Avoid dynamic_cast here.)
<2>Get()返回值错误，由于定位错了导致bug的代码段（以为在Put()函数中），一直以为是cast的问题，消耗了很多时间才发现是Get()函数的代码段出了问题。
   当然，即使是很弱智的bug，在debug过程中得到的“附属品”也是很有价值的，而且在debug的过程中也加深了对于代码细节的理解。
<3>未考虑key.empty()为true的情况（ps：leetcode题中经常遇到这种bug，感觉通过加强算法的“统一性”从而减少特殊情形的复杂度挺重要的，在Remove()函数中可以完成这种统一）。
<4>将TrieNodeWithValue转化为TrieNode的方法有缺陷（仅改变is_value_node），而Clone()方法的内部逻辑考虑到了value_依然存在而将is_value_node赋值为true。
   我深入到Clone()的具体细节才发现了该bug出现的原因，这是由于函数抽象了实现的细节导致coder理解偏差导致的。这个bug我觉得比较好的解决方案我无法用代码写出来，于是
   用了一个比较“丑陋”的方法，有点改变了Clone()函数的本意。（我感觉难点在于如何将TrieNodeWithValue转化为TrieNode）
<5>C++真挺难的……
*/
