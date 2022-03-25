#pragma once
#include <rocksdb/rocksdb_namespace.h>

#include <array>
#include <map>
#include <string>

namespace ROCKSDB_NAMESPACE {

// TODO: reduce binary code size

template <size_t ValueSize>
class BaseTypeErasureMap
    : public std::map<std::string, std::array<unsigned char, ValueSize> > {
 protected:
  void (*m_destruct)(void*);
  void (*m_construct)(void*);
  void (*m_copy_cons)(void* dst, const void* src);
  void (*m_copy_assign)(void* dst, const void* src);
  void (*m_move_cons)(void* dst, void* src);
  void (*m_move_assign)(void* dst, void* src);
  using super::operator[];  // disable
 public:
  ~BaseTypeErasureMap() {
    for (auto& kv : *this) {
      m_destruct(kv.second.data());
    }
  }
  typename super::iterator do_emplace_default_cons(const std::string& key) {
    auto ib = super::emplace(key, std::array<T, unsigned char>());
    if (ib.second) {
      m_default_cons(ib.first->second.data());
    }
    return ib.first;
  }
  std::pair<typename super::iterator, bool> do_emplace_move(
      const std::string& key, void* valptr) {
    auto ib = super::emplace(key, std::array<T, unsigned char>());
    if (ib.second) {
      m_move_cons(ib.first->second.data(), valptr);
    }
    return ib;
  }
  std::pair<typename super::iterator, bool> do_emplace_copy(
      const std::string& key, void* valptr) {
    auto ib = super::emplace(key, std::array<T, unsigned char>());
    if (ib.second) {
      m_copy_cons(ib.first->second.data(), valptr);
    }
    return ib;
  }
};

template <class T>
class TypeErasureMap : public BaseTypeErasureMap<sizeof(T)> {
  typedef BaseTypeErasureMap<sizeof(T)> super;
  static void f_copy_cons(void* dst, void* src) { new (dst) T(*(T*)src); }
  static void f_copy_assign(void* dst, void* src) { *(T*)(dst) = *(T*)src; }
  static void f_move_cons(void* dst, void* src) {
    new (dst) T(std::move(*(T*)src));
  }
  static void f_move_assign(void* dst, void* src) {
    *(T*)(dst) = std::move(*(T*)src);
  }
  static void f_default_cons(void* obj) { new (obj) T(); }
  static void f_destruct(void* obj) { ((T*)obj)->~T(); }

 public:
  TypeErasureMap() {
    m_copy_cons = &f_copy_cons;
    m_copy_assign = &f_copy_assign;
    m_move_cons = &f_move_cons;
    m_move_assign = &f_move_assign;
    m_default_cons = &f_default_cons;
    m_destruct = &f_destruct;
  }
  T& value(typename super::iterator iter) {
    return *reinterpret_cast<T*>(iter->second);
  }
  const T& value(typename super::const_iterator iter) {
    return *reinterpret_cast<const T*>(iter->second);
  }
  std::pair<typename super::iterator, bool> emplace(const std::string& key,
                                                    T&& value) {
    return do_emplace_move(key, &value);
  }
  std::pair<typename super::iterator, bool> emplace(const std::string& key,
                                                    const T& value) {
    return do_emplace_copy(key, &value);
  }
  /*
    T& operator[](const std::string& key) {
      return *reinterpret_cast<const
    T*>(do_emplace_default_cons(key)->second.data());
    }
  */
};

}  // namespace ROCKSDB_NAMESPACE
