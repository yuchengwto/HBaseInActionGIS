// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: WithinFilterProtos.proto

package filter;

public final class WithinFilterProtos {
  private WithinFilterProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface WithinFilterOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required bytes query = 1;
    /**
     * <code>required bytes query = 1;</code>
     */
    boolean hasQuery();
    /**
     * <code>required bytes query = 1;</code>
     */
    com.google.protobuf.ByteString getQuery();
  }
  /**
   * Protobuf type {@code WithinFilter}
   */
  public static final class WithinFilter extends
      com.google.protobuf.GeneratedMessage
      implements WithinFilterOrBuilder {
    // Use WithinFilter.newBuilder() to construct.
    private WithinFilter(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private WithinFilter(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final WithinFilter defaultInstance;
    public static WithinFilter getDefaultInstance() {
      return defaultInstance;
    }

    public WithinFilter getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private WithinFilter(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              query_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return WithinFilterProtos.internal_static_WithinFilter_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return WithinFilterProtos.internal_static_WithinFilter_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              WithinFilter.class, Builder.class);
    }

    public static com.google.protobuf.Parser<WithinFilter> PARSER =
        new com.google.protobuf.AbstractParser<WithinFilter>() {
      public WithinFilter parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new WithinFilter(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<WithinFilter> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required bytes query = 1;
    public static final int QUERY_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString query_;
    /**
     * <code>required bytes query = 1;</code>
     */
    public boolean hasQuery() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required bytes query = 1;</code>
     */
    public com.google.protobuf.ByteString getQuery() {
      return query_;
    }

    private void initFields() {
      query_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasQuery()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, query_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, query_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof WithinFilter)) {
        return super.equals(obj);
      }
      WithinFilter other = (WithinFilter) obj;

      boolean result = true;
      result = result && (hasQuery() == other.hasQuery());
      if (hasQuery()) {
        result = result && getQuery()
            .equals(other.getQuery());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasQuery()) {
        hash = (37 * hash) + QUERY_FIELD_NUMBER;
        hash = (53 * hash) + getQuery().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static WithinFilter parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static WithinFilter parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static WithinFilter parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static WithinFilter parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static WithinFilter parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static WithinFilter parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static WithinFilter parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static WithinFilter parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static WithinFilter parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static WithinFilter parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(WithinFilter prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code WithinFilter}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements WithinFilterOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return WithinFilterProtos.internal_static_WithinFilter_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return WithinFilterProtos.internal_static_WithinFilter_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                WithinFilter.class, Builder.class);
      }

      // Construct using filter.WithinFilterProtos.WithinFilter.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        query_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return WithinFilterProtos.internal_static_WithinFilter_descriptor;
      }

      public WithinFilter getDefaultInstanceForType() {
        return WithinFilter.getDefaultInstance();
      }

      public WithinFilter build() {
        WithinFilter result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public WithinFilter buildPartial() {
        WithinFilter result = new WithinFilter(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.query_ = query_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof WithinFilter) {
          return mergeFrom((WithinFilter)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(WithinFilter other) {
        if (other == WithinFilter.getDefaultInstance()) return this;
        if (other.hasQuery()) {
          setQuery(other.getQuery());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasQuery()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        WithinFilter parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (WithinFilter) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required bytes query = 1;
      private com.google.protobuf.ByteString query_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes query = 1;</code>
       */
      public boolean hasQuery() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required bytes query = 1;</code>
       */
      public com.google.protobuf.ByteString getQuery() {
        return query_;
      }
      /**
       * <code>required bytes query = 1;</code>
       */
      public Builder setQuery(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        query_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes query = 1;</code>
       */
      public Builder clearQuery() {
        bitField0_ = (bitField0_ & ~0x00000001);
        query_ = getDefaultInstance().getQuery();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:WithinFilter)
    }

    static {
      defaultInstance = new WithinFilter(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:WithinFilter)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_WithinFilter_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_WithinFilter_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\030WithinFilterProtos.proto\"\035\n\014WithinFilt" +
      "er\022\r\n\005query\030\001 \002(\014B$\n\006filterB\022WithinFilte" +
      "rProtosH\001\210\001\001\240\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_WithinFilter_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_WithinFilter_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_WithinFilter_descriptor,
              new String[] { "Query", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}