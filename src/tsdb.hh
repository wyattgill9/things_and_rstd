#pragma once

#include <absl/container/flat_hash_map.h>

#include "utils.hh"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <initializer_list>
#include <span>
#include <string>
#include <utility>
#include <vector>

using TypeHandle = u32;
using Timestamp  = i64;

enum class TypeKind : u8 {
    U64, U32, U16, U8,
    I64, I32, I16, I8,
    F64, F32,
    BOOL,
    NUM_PRIMITIVES,

    STRUCT,
};

struct TypeMeta {
    TypeKind    kind_;
    std::string name_;
    u32         size_;
    u32         alignment_;

    std::vector<TypeHandle> fields_;
    std::vector<u32>        field_offsets_;
};

[[nodiscard]]
static constexpr auto align_up(u32  value,
                                u32  alignment) noexcept -> u32 {
    assert(alignment != 0 && (alignment & (alignment - 1)) == 0);
    return (value + alignment - 1) & ~(alignment - 1);
}

class TSDB;

class Column {
public:
    explicit Column(std::string name,
                    u32         element_size,
                    u32         capacity_hint = 0)
        : data_()
        , elem_size_(element_size)
        , name_(std::move(name))
    {
        assert(elem_size_ > 0);
        if (capacity_hint > 0) {
            data_.reserve(static_cast<size_t>(elem_size_) * capacity_hint);
        }
    }

    auto append(const std::byte* __restrict__ source,
                size_t num) -> void {
        assert(source != nullptr);
        const size_t byte_count = elem_size_ * num;
        const size_t old_size   = data_.size();
        data_.resize(old_size + byte_count);
        std::memcpy(data_.data() + old_size, source, byte_count);
    }

    [[nodiscard]] auto elem_slice(size_t start_pos,
                                  size_t end_pos) const noexcept -> std::span<const std::byte> {
        assert(end_pos >= start_pos);
        assert(end_pos * elem_size_ <= data_.size());
        return { elem_ptr(start_pos), elem_ptr(end_pos) };
    }

    [[nodiscard]] auto name()      const noexcept -> const std::string& { return name_;      }
    [[nodiscard]] auto elem_size() const noexcept -> u32                { return elem_size_; }
    [[nodiscard]] auto num_rows()  const noexcept -> size_t             { return data_.size() / elem_size_; }
    [[nodiscard]] auto raw_data()  const noexcept -> const std::byte*   { return data_.data(); }

private:
    [[nodiscard]] auto elem_ptr(size_t pos) const noexcept -> const std::byte* {
        return data_.data() + pos * elem_size_;
    }

    std::vector<std::byte> data_;
    u32                    elem_size_;
    std::string            name_;
};

class Table {
public:
    explicit Table(TypeHandle type) noexcept
        : struct_type_(type)
    {}

    auto insert(const std::byte* __restrict__ data,
                const TypeMeta& meta) -> void
    {
        assert(data != nullptr);
        assert(meta.kind_ == TypeKind::STRUCT);

        if (columns_.empty() == true) {
            init_columns(meta);
        }

        for (size_t i = 0; i < meta.fields_.size(); ++i) {
            columns_[i].append(data + meta.field_offsets_[i], 1); // append 1
        }
    }

    [[nodiscard]]
    auto query_first(std::byte* __restrict__ dest,
                     const TypeMeta& meta) const noexcept -> bool
    {
        if (!columns_.empty() && columns_.front().num_rows() > 0) [[likely]] {
            return false;
        }

        const size_t n = meta.fields_.size();
        for (size_t i = 0; i < n; ++i) {
            auto slice = columns_[i].elem_slice(0, 1);
            std::memcpy(dest + meta.field_offsets_[i],
                        slice.data(),
                        columns_[i].elem_size());
        }
        return true;
    }

    [[nodiscard]] auto num_rows() const noexcept -> size_t {
        return columns_.empty() ? 0 : columns_[0].num_rows();
    }

private:
    auto init_columns(const TypeMeta& meta) -> void {
        columns_.reserve(meta.fields_.size());
        for (size_t i = 0; i < meta.fields_.size(); ++i) {
            // name = "<struct>.<field_index>"
            columns_.emplace_back(
                meta.name_ + "." + std::to_string(i),
                static_cast<u32>(meta.fields_[i]),
                0);
        }
    }

    TypeHandle          struct_type_;
    std::vector<Column> columns_;

    friend class TSDB;
};

class TSDB {
public:
    explicit TSDB(size_t est_num_types = 0) {
        types_.reserve(est_num_types + static_cast<size_t>(TypeKind::NUM_PRIMITIVES));
        register_primitives();
    }

    ~TSDB()                      = default;
    TSDB(const TSDB&)            = delete;
    TSDB(TSDB&&)                 = delete;
    TSDB& operator=(const TSDB&) = delete;
    TSDB& operator=(TSDB&&)      = delete;

    [[nodiscard]] auto size_of (TypeHandle h) const noexcept -> u32      { return types_[h].size_;      }
    [[nodiscard]] auto align_of(TypeHandle h) const noexcept -> u32      { return types_[h].alignment_; }
    [[nodiscard]] auto type_of (TypeHandle h) const noexcept -> TypeKind { return types_[h].kind_;      }

    auto register_struct(
            std::string name,
            std::initializer_list<std::pair<std::string, const TypeHandle>> fields)
        -> TypeHandle
    {
        u32 struct_size  = 0;
        u32 struct_align = 1;

        std::vector<TypeHandle> field_types;
        std::vector<u32>        field_offsets;
        field_types.reserve(fields.size());
        field_offsets.reserve(fields.size());

        for (auto&& [name, type] : fields) {
            const auto& fm = types_[type];
            struct_align = std::max(struct_align, fm.alignment_);
            struct_size  = align_up(struct_size, fm.alignment_);

            field_offsets.push_back(struct_size);
            struct_size += fm.size_;
            field_types.push_back(type);
        }

        struct_size = align_up(struct_size, struct_align);

        const auto handle = static_cast<TypeHandle>(types_.size());

        types_.push_back(TypeMeta{
            .kind_          = TypeKind::STRUCT,
            .name_          = std::move(name),
            .size_          = struct_size,
            .alignment_     = struct_align,
            .fields_        = std::move(field_types),
            .field_offsets_ = std::move(field_offsets),
        });

        return handle;
    }

    template<typename T>
    auto insert(T val, TypeHandle type) -> void {
        assert(type_of(type) == TypeKind::STRUCT);
        assert(sizeof(T) == size_of(type));

        const auto& meta = types_[type];
        Table& table     = get_or_create_table(type);

        if (table.columns_.empty() == true) [[unlikely]] {
            table.columns_.reserve(meta.fields_.size());
            for (size_t i = 0; i < meta.fields_.size(); ++i) {
                table.columns_.emplace_back(
                    meta.name_ + "." + std::to_string(i),
                    types_[meta.fields_[i]].size_);
            }
        }

        const auto* raw = reinterpret_cast<const std::byte*>(&val);
        const size_t n   = meta.fields_.size();
        for (size_t i = 0; i < n; ++i) {
            table.columns_[i].append(raw + meta.field_offsets_[i], 1); // num = 1
        }
    }

    template<typename T>
    auto query_first(T& dest, TypeHandle type) const -> void {
        assert(sizeof(T) == size_of(type));

        const auto&  meta  = types_[type];
        const Table& table = tables_.at(type);

        assert(table.num_rows() > 0);

        auto* raw      = reinterpret_cast<std::byte*>(&dest);
        const size_t n = meta.fields_.size();
        for (size_t i = 0; i < n; ++i) {
            auto slice = table.columns_[i].elem_slice(0, 1);
            std::memcpy(raw + meta.field_offsets_[i],
                        slice.data(),
                        table.columns_[i].elem_size());
        }
    }

    static constexpr TypeHandle U64  = 0;
    static constexpr TypeHandle U32  = 1;
    static constexpr TypeHandle U16  = 2;
    static constexpr TypeHandle U8   = 3;
    static constexpr TypeHandle I64  = 4;
    static constexpr TypeHandle I32  = 5;
    static constexpr TypeHandle I16  = 6;
    static constexpr TypeHandle I8   = 7;
    static constexpr TypeHandle F64  = 8;
    static constexpr TypeHandle F32  = 9;
    static constexpr TypeHandle BOOL = 10;

private:
    auto get_or_create_table(TypeHandle type) -> Table& {
        auto [it, inserted] = tables_.try_emplace(type, type);
        return it->second;
    }

    auto register_primitives() -> void {
        types_ = {
            { TypeKind::U64,  "u64",  8, 8, {}, {} },
            { TypeKind::U32,  "u32",  4, 4, {}, {} },
            { TypeKind::U16,  "u16",  2, 2, {}, {} },
            { TypeKind::U8,   "u8",   1, 1, {}, {} },
            { TypeKind::I64,  "i64",  8, 8, {}, {} },
            { TypeKind::I32,  "i32",  4, 4, {}, {} },
            { TypeKind::I16,  "i16",  2, 2, {}, {} },
            { TypeKind::I8,   "i8",   1, 1, {}, {} },
            { TypeKind::F64,  "f64",  8, 8, {}, {} },
            { TypeKind::F32,  "f32",  4, 4, {}, {} },
            { TypeKind::BOOL, "bool", 1, 1, {}, {} },
        };
    }

    std::vector<TypeMeta>                  types_;
    absl::flat_hash_map<TypeHandle, Table> tables_;
};
