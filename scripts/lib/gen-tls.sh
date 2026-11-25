# TLS material for one stack (Shift 11, D100), sourced by scripts/lib/compose-stack.sh and scripts/kind-smoke.sh: a
# fresh certificate authority, and one server certificate per service signed by it, each naming the hosts it answers
# to. Nothing is reused between runs or committed; the CA's key is deleted once the certificates are signed, so not even
# the stack that holds them can issue another.
#   tls_generate DIR SERVICE=host,host,... [SERVICE=host,...]
# writes DIR/ca.crt and DIR/<service>.crt and DIR/<service>.key (PKCS#8, unencrypted), each file readable by anyone
# who can open DIR (the caller keeps DIR private on the host).
tls_generate() {
    local dir=$1
    shift
    mkdir -p "$dir"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 -subj "/CN=Driftline stack CA" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -keyout "$dir/ca.key" -out "$dir/ca.crt" 2> /dev/null
    local spec service hosts san host
    for spec in "$@"; do
        service=${spec%%=*}
        hosts=${spec#*=}
        san=""
        IFS=, read -r -a names <<< "$hosts"
        for host in "${names[@]}"; do
            if [[ $host =~ ^[0-9.]+$ ]]; then
                san+="IP:$host,"
            else
                san+="DNS:$host,"
            fi
        done
        openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=${names[0]}" \
            -keyout "$dir/$service.key" -out "$dir/$service.csr" 2> /dev/null
        openssl x509 -req -in "$dir/$service.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 30 \
            -extfile <(printf 'subjectAltName=%s\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n' "${san%,}") \
            -out "$dir/$service.crt" 2> /dev/null
        # PKCS#8, the form Java, Kafka and PostgreSQL all read
        openssl pkcs8 -topk8 -nocrypt -in "$dir/$service.key" -out "$dir/$service.key.p8"
        mv "$dir/$service.key.p8" "$dir/$service.key"
        rm -f "$dir/$service.csr"
    done
    rm -f "$dir/ca.key" "$dir/ca.srl"
    chmod 644 "$dir"/*
}

# The TLS files of a whole stack under DIR (Shift 11, D100), as Compose and the kind overlay mount them:
#   tls-app/      ca.crt, server.crt, server.key, truststore.p12   Driftline's services (DIR/app gets the Kafka password)
#   tls-sandbox/  ca.crt, server.p12, keystore-password, truststore.p12
#   tls-client/   ca.crt, truststore.p12                            the migrate and verify roles
#   postgres/     ca.crt, server.crt, server.key
#   kafka/        ca.crt, kafka.p12, keystore-password, key-password, jaas.conf, client.properties (the admin login)
#   collector/    ca.crt, server.crt, server.key                    the OpenTelemetry collector (Compose's obs profile)
#   ca.crt, truststore.p12                                          for tools on the host
#   tls_stack DIR APP_HOSTS SANDBOX_HOSTS
# Truststore passwords protect integrity only (a truststore holds no secret); every other password is random.
tls_stack() {
    local s=$1 app_hosts=$2 sandbox_hosts=$3 d
    for d in app tls-app tls-sandbox tls-client postgres kafka collector; do
        mkdir -p "$s/$d"
        chmod 755 "$s/$d"
    done
    tls_generate "$s/tls" "app=$app_hosts" postgres=postgres kafka=kafka,localhost "sandbox=$sandbox_hosts" \
        collector=otel-collector
    cp "$s/tls/ca.crt" "$s/ca.crt"
    for d in tls-app tls-sandbox tls-client postgres kafka collector; do
        cp "$s/tls/ca.crt" "$s/$d/ca.crt"
    done
    cp "$s/tls/app.crt" "$s/tls-app/server.crt"
    cp "$s/tls/app.key" "$s/tls-app/server.key"
    cp "$s/tls/postgres.crt" "$s/postgres/server.crt"
    cp "$s/tls/postgres.key" "$s/postgres/server.key"
    cp "$s/tls/collector.crt" "$s/collector/server.crt"
    cp "$s/tls/collector.key" "$s/collector/server.key"
    for d in tls-app tls-sandbox tls-client .; do
        keytool -importcert -noprompt -alias driftline-ca -file "$s/tls/ca.crt" -keystore "$s/$d/truststore.p12" \
            -storetype PKCS12 -storepass changeit > /dev/null 2>&1
    done
    local sandbox_password kafka_keystore kafka_app kafka_admin kafka_broker
    sandbox_password=$(openssl rand -hex 16)
    openssl pkcs12 -export -in "$s/tls/sandbox.crt" -inkey "$s/tls/sandbox.key" -name sandbox \
        -passout "pass:$sandbox_password" -out "$s/tls-sandbox/server.p12"
    printf '%s' "$sandbox_password" > "$s/tls-sandbox/keystore-password"
    # Kafka: TLS with password logins (SASL PLAIN, only ever over TLS); the broker's keystore is PKCS#12, as its image
    # wants, and the clients trust the CA's PEM file
    kafka_keystore=$(openssl rand -hex 16)
    kafka_app=$(openssl rand -hex 16)
    kafka_admin=$(openssl rand -hex 16)
    kafka_broker=$(openssl rand -hex 16)
    openssl pkcs12 -export -in "$s/tls/kafka.crt" -inkey "$s/tls/kafka.key" -name kafka \
        -passout "pass:$kafka_keystore" -out "$s/kafka/kafka.p12"
    printf '%s' "$kafka_keystore" > "$s/kafka/keystore-password"
    printf '%s' "$kafka_keystore" > "$s/kafka/key-password"
    printf 'KafkaServer {\n    org.apache.kafka.common.security.plain.PlainLoginModule required\n    username="broker" password="%s"\n    user_broker="%s"\n    user_driftline="%s"\n    user_admin="%s";\n};\n' \
        "$kafka_broker" "$kafka_broker" "$kafka_app" "$kafka_admin" > "$s/kafka/jaas.conf"
    printf 'security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="%s";\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/secrets/ca.crt\n' \
        "$kafka_admin" > "$s/kafka/client.properties"
    # the driftline login as a client file too, for scripts/kafka-acl-check.sh (D101)
    printf 'security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="driftline" password="%s";\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/secrets/ca.crt\n' \
        "$kafka_app" > "$s/kafka/driftline-client.properties"
    printf '%s' "$kafka_app" > "$s/app/driftline.kafka.password"
    rm -rf "$s/tls"
    chmod 644 "$s"/tls-*/* "$s"/postgres/* "$s"/kafka/* "$s"/collector/* "$s/ca.crt" "$s/truststore.p12" \
        "$s/app/driftline.kafka.password"
}
