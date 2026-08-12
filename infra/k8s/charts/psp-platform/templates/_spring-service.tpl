{{/*
  Shared render for the seven Spring services.

  WHY THIS LIVES IN THE UMBRELLA CHART AND NOT IN EACH SUBCHART
  Helm merges the template namespace across the whole chart tree, so a `define` here is callable
  from every subchart under charts/. Seven copies of the same 120-line Deployment is how charts
  rot: the day someone fixes a probe they fix it in three of seven files. Each service subchart
  therefore contains only what actually DIFFERS - its port, its Strimzi KafkaUser, its resources,
  and its application-k8s.yml - in its own values.yaml, which is the file you want to be reading
  when you ask "what is this service configured with".

  The alternative Helm idiom is a library chart (Chart.type: library) declared as a dependency by
  each subchart. That is the more portable answer and the wrong one here: dependencies need
  `helm dependency update`, which needs a chart repository or a file:// vendoring step, and this
  phase deliberately has no registry of any kind.
*/}}

{{/* Common labels. `.Chart.Name` inside a subchart is the SUBCHART's name, which is what we want. */}}
{{- define "psp.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: psp-platform
{{- end -}}

{{- define "psp.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* ------------------------------------------------------------------------------------------
  ConfigMap: one file, /config/application-k8s.yml.

  HOW CONFIG REACHES THE POD, and why it is a mounted file rather than a wall of env vars:

    SPRING_PROFILES_ACTIVE=k8s              activates the profile (api-gateway REQUIRES it -
                                            KnownProfileGuard fails startup on an unknown profile)
    SPRING_CONFIG_ADDITIONAL_LOCATION=/config/
                                            appends /config to Spring's config locations, AFTER
                                            the classpath, so /config/application-k8s.yml has the
                                            HIGHEST precedence of the four candidate documents
                                            (classpath application.yml, /config/application.yml,
                                            classpath application-k8s.yml, /config/application-k8s.yml).

  Env-var overrides would work for scalars but not for api-gateway, whose k8s profile ships a
  six-entry route LIST inside the jar; a partially-specified list in a higher-precedence source
  merges per-index and produces routes assembled from two files. A mounted profile document that
  simply does not mention `routes` leaves the jar's list intact, which is the behaviour we want.
------------------------------------------------------------------------------------------ */}}
{{- define "psp.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Chart.Name }}-config
  labels:
    {{- include "psp.labels" . | nindent 4 }}
data:
  application-k8s.yml: |
{{ tpl .Values.config . | indent 4 }}
{{- end -}}

{{/* ------------------------------------------------------------------------------------------
  Deployment.
------------------------------------------------------------------------------------------ */}}
{{- define "psp.deployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Chart.Name }}
  labels:
    {{- include "psp.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.replicas | default 1 }}
  selector:
    matchLabels:
      {{- include "psp.selectorLabels" . | nindent 6 }}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      # maxSurge 1 / maxUnavailable 0 is the only combination that makes the readiness probe
      # load-bearing: the new pod must report Ready before the old one is removed from the
      # Service's endpoints. With maxUnavailable 1 the old pod goes first and a shallow probe
      # would never be noticed.
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        {{- include "psp.selectorLabels" . | nindent 8 }}
      annotations:
        # Roll the pods when the config changes. Without this, `helm upgrade` after editing a
        # service's application-k8s.yml updates the ConfigMap and changes nothing - the running
        # JVM read the file once, at startup, and Spring does not watch it.
        psp.example.com/config-checksum: {{ tpl .Values.config . | sha256sum }}
    spec:
      # Kubernetes injects a docker-link-style env var for every Service in the namespace:
      # a Service called `schema-registry` becomes SCHEMA_REGISTRY_PORT=tcp://10.96.x.x:8081,
      # SCHEMA_REGISTRY_PORT_8081_TCP_ADDR=..., and so on. Confluent's images configure themselves
      # from exactly that prefix, so the Service's own name silently poisons its container's
      # configuration and it exits 1 after printing "PORT is deprecated". Off for every pod here,
      # not just that one: the mechanism predates DNS-based discovery, nothing in this chart uses
      # it, and the failure it causes is extremely hard to read.
      enableServiceLinks: false
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: {{ .Chart.Name }}
          image: "{{ .Values.global.imageRepository }}/{{ .Chart.Name }}:{{ .Values.global.imageTag }}"
          imagePullPolicy: {{ .Values.global.imagePullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.port }}
          env:
            - name: SPRING_PROFILES_ACTIVE
              value: k8s
            - name: SPRING_CONFIG_ADDITIONAL_LOCATION
              value: "file:/config/"
            {{- if .Values.kafkaUser }}
            # THE credential wiring. `{{ .Values.kafkaUser }}` is a Secret the Strimzi User
            # Operator generated from infra/k8s/kafka/users/*.yaml - not created by this chart,
            # not copied into it, not in git. Helm references it by name; if the Secret is deleted
            # the User Operator mints a new password into the same name and a pod restart picks it
            # up. The key is `sasl.jaas.config`, a complete JAAS line, so the password never
            # exists as a standalone value in this process's environment.
            - name: SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG
              valueFrom:
                secretKeyRef:
                  name: {{ .Values.kafkaUser }}
                  key: {{ .Values.global.kafka.saslJaasConfigSecretKey }}
            {{- end }}
            {{- with .Values.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          # ------------------------------------------------------------------------------------
          # Probes. All three hit Spring Boot Actuator; what differs is WHICH health group.
          #
          #   startup    /actuator/health/liveness  - holds the other two off until the app has
          #                                           answered once. Generous budget because
          #                                           Flyway + Hibernate + the first Kafka admin
          #                                           round-trip is 30-60 s on kind.
          #   liveness   /actuator/health/liveness  - "restarting would help". Deliberately
          #                                           SHALLOW: it must not include Kafka, or a
          #                                           broker rollout restarts every service pod.
          #   readiness  /actuator/health/readiness - "send me traffic / count me as a working
          #                                           member of this deployment". This one is
          #                                           deep: see readinessInclude in the subchart's
          #                                           values.yaml and infra/k8s/README.md.
          # ------------------------------------------------------------------------------------
          startupProbe:
            httpGet:
              path: /actuator/health/liveness
              port: http
            periodSeconds: {{ .Values.global.probes.startup.periodSeconds }}
            failureThreshold: {{ .Values.global.probes.startup.failureThreshold }}
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: http
            periodSeconds: {{ .Values.global.probes.liveness.periodSeconds }}
            timeoutSeconds: {{ .Values.global.probes.liveness.timeoutSeconds }}
            failureThreshold: {{ .Values.global.probes.liveness.failureThreshold }}
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: http
            periodSeconds: {{ .Values.global.probes.readiness.periodSeconds }}
            timeoutSeconds: {{ .Values.global.probes.readiness.timeoutSeconds }}
            failureThreshold: {{ .Values.global.probes.readiness.failureThreshold }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: {{ .Values.global.readOnlyRootFilesystem }}
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: config
          configMap:
            name: {{ .Chart.Name }}-config
        - name: tmp
          emptyDir: {}
{{- end -}}

{{/* ------------------------------------------------------------------------------------------
  Service. THIS OBJECT IS THE POINT OF ADR-0009 - see infra/k8s/README.md, "What native
  Kubernetes discovery replaced". A stable name, a stable ClusterIP, and an endpoint list that
  kube-proxy keeps in sync with the readiness probe above. api-gateway's k8s profile dials these
  names directly; nothing registers anywhere.
------------------------------------------------------------------------------------------ */}}
{{- define "psp.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .Chart.Name }}
  labels:
    {{- include "psp.labels" . | nindent 4 }}
spec:
  type: ClusterIP
  selector:
    {{- include "psp.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .Values.port }}
      targetPort: http
{{- end -}}

{{/* ------------------------------------------------------------------------------------------
  The three fragments of Spring config every service shares, injected into each subchart's
  `config` string with {{ include "psp.commonConfig" . }} so that a change to (say) the actuator
  probe wiring happens once.
------------------------------------------------------------------------------------------ */}}
{{- define "psp.commonConfig" -}}
management:
  endpoint:
    health:
      # Creates /actuator/health/liveness and /actuator/health/readiness.
      probes:
        enabled: true
      # Unauthenticated detail on a laptop cluster, so `kubectl exec ... curl .../health/readiness`
      # names the listener that is down instead of just saying DOWN. Would not ship like this.
      show-details: always
      group:
        readiness:
          include: {{ .Values.readinessInclude | quote }}
        liveness:
          # Left at Spring Boot's default (livenessState only) ON PURPOSE. Anything Kafka-shaped
          # in here turns a broker restart into a mass pod restart.
          include: livenessState
  tracing:
    sampling:
      # Tempo is not deployed in phase 2. Sampling 0 means the OTLP exporter is never invoked, so
      # the Tracer bean and W3C traceparent propagation keep working while nothing tries to reach
      # a collector that is not there.
      probability: 0.0
{{- end -}}

{{/* Emitted at column 0; callers place it under `spring:` with `| nindent 2`. */}}
{{- define "psp.kafkaClientConfig" -}}
kafka:
  bootstrap-servers: {{ .Values.global.kafka.bootstrapServers }}
  properties:
    security.protocol: {{ .Values.global.kafka.securityProtocol }}
    sasl.mechanism: {{ .Values.global.kafka.saslMechanism }}
    # sasl.jaas.config is NOT here. It arrives as the environment variable
    # SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG, sourced from the Strimzi Secret named
    # `{{ .Values.kafkaUser }}` - see the Deployment template.
{{- end -}}
