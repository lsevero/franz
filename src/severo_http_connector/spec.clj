(ns severo-http-connector.spec)

(def serialization-spec
  [:multi {:dispatch :type}
   [:json [:map 
           [:type keyword?]]]
   [:avro [:map 
           [:type keyword?]
           [:consumer-spec [:or string? map?]]
           [:producer-spec [:or string? map?]] 
           ]]
   ])

(def get-route-spec
  [:map {:closed true}
   [:send-topic {:optional true} string?]
   [:listen-topic {:optional true} string?]
   [:poll-duration {:optional true} pos-int?]
   [:timeout {:optional true} pos-int?]
   [:partitions {:optional true} pos-int?]
   [:replication {:optional true} pos-int?]
   [:summary {:optional true} string?]
   [:consumer {:optional true} [:map-of :string :string]]
   [:producer {:optional true} [:map-of :string :string]]
   [:serialization {:optional true} serialization-spec]
   [:parameters {:optional true} [:map {:closed true}
                                  [:header {:optional true} any?]
                                  [:path {:optional true} any?]
                                  [:query {:optional true} any?]]]])

(def post-route-spec
  [:map {:closed true}
   [:send-topic {:optional true} string?]
   [:listen-topic {:optional true} string?]
   [:poll-duration {:optional true} pos-int?]
   [:timeout {:optional true} pos-int?]
   [:partitions {:optional true} pos-int?]
   [:replication {:optional true} pos-int?]
   [:mode {:optional true} [:enum :request-response :fire-and-forget]]
   [:summary {:optional true} string?]
   [:consumer {:optional true} [:map-of :string :string]]
   [:producer {:optional true} [:map-of :string :string]]
   [:serialization {:optional true} serialization-spec]
   [:parameters {:optional true} [:map {:closed true}
                                  [:header {:optional true} any?]
                                  [:path {:optional true} any?]
                                  [:query {:optional true} any?]
                                  [:body any?]]]])

(def config-spec
  [:map
   [:kafka [:map {:closed true}
            [:consumer [:map-of :string :string]]
            [:producer [:map-of :string :string]]
            ]]
   [:http [:and
           [:map
            [:min-threads {:optional true} pos-int?]
            [:max-threads {:optional true} pos-int?]
            [:max-idle-time {:optional true} pos-int?]
            [:request-header-size {:optional true} pos-int?]
            [:port {:optional true} [:and pos-int? [:< 65535]]]
            ]
           [:fn {:error/message "max-threads need to be greater than min-threads"} (fn [{:keys [min-threads max-threads]}] (> max-threads min-threads))]
           ]]
   [:swagger [:map {:closed true}
              [:enabled? {:optional true} boolean?]
              [:title {:optional true} string?]
              [:description {:optional true} string?]
              [:path {:optional true} string?]
              ]]
   [:defaults [:map {:closed true}
               [:send-topic {:optional true} string?]
               [:listen-topic {:optional true} string?]
               [:poll-duration {:optional true} pos-int?]
               [:timeout {:optional true} pos-int?]
               [:partitions {:optional true} pos-int?]
               [:replication {:optional true} pos-int?]
               ]]
   [:routes 
    [:and [:map-of :string [:and
                            [:map {:closed true}
                             [:get {:optional true} get-route-spec]
                             [:head {:optional true} get-route-spec]
                             [:post {:optional true} post-route-spec]
                             [:put {:optional true} post-route-spec]
                             [:delete {:optional true} post-route-spec]
                             [:connect {:optional true} post-route-spec]
                             [:options {:optional true} post-route-spec]
                             [:trace {:optional true} post-route-spec]
                             [:patch {:optional true} post-route-spec]]
                            [:fn {:error/message "method map cannot be empty"} (fn [m] (not (empty? m)))]]]
     [:fn {:error/message "route map cannot be empty"} (fn [m] (not (empty? m)))]]]])
