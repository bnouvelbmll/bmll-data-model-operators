{{- define "dmo.name" -}}{{ .Release.Name }}-datamodel-operators{{- end -}}
{{- define "dmo.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
