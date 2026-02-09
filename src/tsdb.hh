#pragma once

#include "absl/container/flat_hash_map.h"

#include "utils.hh"

#include <cstring>
#include <span>
#include <utility>
#include <vector>
#include <string>
#include <initializer_list>

template <std::unsigned_integral T>
[[nodiscard]] constexpr auto align_up(T value, T alignment) noexcept -> T {
    assert(alignment != 0);
    assert(std::has_single_bit(alignment));

    return (value + alignment - 1) & ~(alignment - 1);
}

class Schema;
class TSDB;

struct TypeHandle {
    constexpr TypeHandle(u32 v) : v_(v) {}

    constexpr friend bool operator==(TypeHandle, TypeHandle) = default;

    template <typename H>
    friend H AbslHashValue(H h, const TypeHandle& t) {
        return H::combine(std::move(h), t.v_);
    }

private:
    friend class Schema;
    friend class TSDB;

    u32 v_;
};

class Schema {
public:
    enum class TypeKind : u8 {
        U8, U16, U32, U64,
        I8, I16, I32, I64,
        F32, F64,
        BOOL,
        TIMESTAMP_NS,

        NUM_PRIMITIVES,

        STRUCT,
    };

    struct TypeMeta {
        u32       size_;
        u32       alignment_;
        u32       field_begin_;
        u16       field_count_;
        TypeKind  kind_;
        std::byte _pad[1];
    };
    static_assert(sizeof(TypeMeta) == 16);

public:
    Schema(size_t est_num_types) {
        const auto prim_count = std::to_underlying(TypeKind::NUM_PRIMITIVES);
        types_.reserve(est_num_types + prim_count);
        type_names_.reserve(est_num_types + prim_count);
        init_primitives();
    }

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {
        u32 struct_size      = 0;
        u32 struct_alignment = 1;

        const u32 field_begin = static_cast<u32>(field_types_.size());

        for (auto&& [field_name, type] : fields) {
            const TypeMeta& meta = meta_of(type);

            struct_alignment = std::max(struct_alignment, meta.alignment_);
            struct_size      = align_up(struct_size, meta.alignment_);

            field_offsets_.push_back(struct_size);
            struct_size += meta.size_;
            field_types_.push_back(type);
            field_names_.push_back(field_name);
        }

        struct_size = align_up(struct_size, struct_alignment);

        const TypeHandle handle{static_cast<u32>(types_.size())};

        types_.push_back(TypeMeta{
            .size_        = struct_size,
            .alignment_   = struct_alignment,
            .field_begin_ = field_begin,
            .field_count_ = static_cast<u16>(fields.size()),
            .kind_        = TypeKind::STRUCT,
        });
        type_names_.push_back(std::move(name));

        return handle;
    }

    [[nodiscard]] auto meta_of (const TypeHandle& type) const -> const TypeMeta& { return types_[type.v_];           }
    [[nodiscard]] auto name_of (const TypeHandle& type) const -> std::string_view { return type_names_[type.v_];     }
    [[nodiscard]] auto kind_of (const TypeHandle& type) const -> TypeKind         { return meta_of(type).kind_;      }
    [[nodiscard]] auto size_of (const TypeHandle& type) const -> size_t           { return meta_of(type).size_;      }
    [[nodiscard]] auto align_of(const TypeHandle& type) const -> size_t           { return meta_of(type).alignment_; }

    [[nodiscard]] auto field_types(const TypeHandle& type) const -> std::span<const TypeHandle> {
        const auto& m = meta_of(type);
        return { field_types_.data() + m.field_begin_, m.field_count_ };
    }
    [[nodiscard]] auto field_offsets(const TypeHandle& type) const -> std::span<const size_t> {
        const auto& m = meta_of(type);
        return { field_offsets_.data() + m.field_begin_, m.field_count_ };
    }
    [[nodiscard]] auto field_names(const TypeHandle& type) const -> std::span<const std::string> {
        const auto& m = meta_of(type);
        return { field_names_.data() + m.field_begin_, m.field_count_ };
    }

private:
    auto init_primitives() -> void {
        auto add = [&](u32 size, std::string_view name) {
            types_.push_back(TypeMeta {size, size, 0, 0, static_cast<TypeKind>(types_.size())});
            type_names_.emplace_back(name);
        };
    
        add(1, "u8" );  add(2, "u16");  add(4, "u32");  add(8, "u64");
        add(1, "i8" );  add(2, "i16");  add(4, "i32");  add(8, "i64");
        add(4, "f32");  add(8, "f64");

        add(1, "bool");
        add(8, "timestamp_ns");
    }

    std::vector<TypeMeta>    types_;
    std::vector<std::string> type_names_;

    std::vector<TypeHandle>  field_types_;
    std::vector<size_t>      field_offsets_;
    std::vector<std::string> field_names_;
};

struct Column {
public:
    Column() = default;
    explicit Column(size_t elem_size) : elem_size_(elem_size) {}

    auto push(const std::byte* data) -> void {
        const size_t old_size = data_.size();
        data_.resize(old_size + elem_size_);
        std::memcpy(data_.data() + old_size, data, elem_size_);
    }

    [[nodiscard]] auto at(size_t row) const -> const std::byte* {
        return data_.data() + row * elem_size_;
    }

    [[nodiscard]] auto row_count() const -> size_t {
        return elem_size_ > 0 ? data_.size() / elem_size_ : 0;
    }

    [[nodiscard]] auto elem_size() const -> size_t { return elem_size_; }

    auto reserve(size_t row_count) -> void {
        data_.reserve(row_count * elem_size_);
    }

private:
    size_t elem_size_ = 0;
    std::vector<std::byte> data_;
};

struct Table {
public:
    Table(std::vector<size_t> field_sizes, std::vector<size_t> field_offsets)
        : field_offsets_(std::move(field_offsets))
    {
        columns_.reserve(field_sizes.size());
        for (size_t sz : field_sizes) {
            columns_.emplace_back(sz);
        }
    }

    auto insert_row(const std::byte* src) -> void {
        for (size_t i = 0; i < columns_.size(); ++i) {
            columns_[i].push(src + field_offsets_[i]);
        }
        ++row_count_;
    }

    auto read_row(size_t row, std::byte* dst) const -> void {
        for (size_t i = 0; i < columns_.size(); ++i) {
            std::memcpy(dst + field_offsets_[i], columns_[i].at(row), columns_[i].elem_size());
        }
    }

    auto reserve(size_t row_count) -> void {
        for (auto& col : columns_) {
            col.reserve(row_count);
        }
    }

    [[nodiscard]] auto row_count() const -> size_t { return row_count_; }

private:
    size_t row_count_ = 0;
    std::vector<size_t> field_offsets_;
    std::vector<Column> columns_;
};

class TSDB {
public:
    TSDB(size_t est_num_types = 1) : schema_(est_num_types) {}

    ~TSDB()           = default;
    TSDB(const TSDB&) = delete;
    TSDB(TSDB&&)      = delete;

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {
        return schema_.register_struct(name, fields);
    }

    template<typename T>
    auto insert(const T& src, TypeHandle type) -> void {
        static_assert(std::is_trivially_copyable_v<T>);

        Table& table = get_or_create_table(type);
        const auto* bytes = reinterpret_cast<const std::byte*>(&src);

        table.insert_row(bytes);
    }

    template<typename T>
    [[nodiscard]] auto query_first(TypeHandle type) const -> T {
        static_assert(std::is_trivially_copyable_v<T>);

        const Table* table = get_table_ptr(type);
        if (table == nullptr || table->row_count() == 0) {
            return T{};
        }

        T result{};
        auto* dst = reinterpret_cast<std::byte*>(&result);
        table->read_row(0, dst);

        return result;
    }

    // Default Types
    constexpr static TypeHandle U8           {std::to_underlying(Schema::TypeKind::U8)};
    constexpr static TypeHandle U16          {std::to_underlying(Schema::TypeKind::U16)};
    constexpr static TypeHandle U32          {std::to_underlying(Schema::TypeKind::U32)};
    constexpr static TypeHandle U64          {std::to_underlying(Schema::TypeKind::U64)};
    constexpr static TypeHandle I8           {std::to_underlying(Schema::TypeKind::I8)};
    constexpr static TypeHandle I16          {std::to_underlying(Schema::TypeKind::I16)};
    constexpr static TypeHandle I32          {std::to_underlying(Schema::TypeKind::I32)};
    constexpr static TypeHandle I64          {std::to_underlying(Schema::TypeKind::I64)};
    constexpr static TypeHandle F32          {std::to_underlying(Schema::TypeKind::F32)};
    constexpr static TypeHandle F64          {std::to_underlying(Schema::TypeKind::F64)};
    constexpr static TypeHandle BOOL         {std::to_underlying(Schema::TypeKind::BOOL)};
    constexpr static TypeHandle TIMESTAMP_NS {std::to_underlying(Schema::TypeKind::TIMESTAMP_NS)};

private:
    [[nodiscard]] auto get_table_ptr(TypeHandle type) const -> const Table* {
        auto it = tables_.find(type);
        if (it != tables_.end()) return &it->second;
        return nullptr;
    }

    [[nodiscard]] auto get_or_create_table(TypeHandle type) -> Table& {
        auto it = tables_.find(type);
        if (it != tables_.end()) {
            return it->second;
        }

        // Amortized Table Creation
        auto ftypes   = schema_.field_types(type);
        auto foffsets = schema_.field_offsets(type);

        std::vector<size_t> field_sizes;
        field_sizes.reserve(ftypes.size());
        for (auto& ft : ftypes) {
            field_sizes.push_back(schema_.size_of(ft));
        }

        return tables_.emplace(type, Table{
            std::move(field_sizes),
            { foffsets.begin(), foffsets.end() }
        }).first->second;
    }

    Schema schema_;
    absl::flat_hash_map<TypeHandle, Table> tables_;
};
