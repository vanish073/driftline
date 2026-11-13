import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/** Fails the build if any Spring artifact reaches a non-`app` module's classpath (DESIGN.md D18). */
@CacheableTask
abstract class VerifyNoSpring : DefaultTask() {
    @get:Input
    abstract val components: ListProperty<String>

    @get:OutputFile
    abstract val report: RegularFileProperty

    @TaskAction
    fun verify() {
        val offenders = components.get().filter { it.startsWith("org.springframework") }.distinct().sorted()
        if (offenders.isNotEmpty()) {
            throw GradleException(
                "Spring is confined to the `app` module (DESIGN.md D18), but this module pulls in: $offenders"
            )
        }
        report.get().asFile.writeText("no Spring artifacts\n")
    }
}
